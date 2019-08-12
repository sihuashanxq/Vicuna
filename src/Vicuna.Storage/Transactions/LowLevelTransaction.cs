using System;
using System.Collections.Generic;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Transactions
{
    public class LowLevelTransaction : IDisposable
    {
        internal long Id { get; }

        internal BufferPool Buffers { get; }

        internal LockManager LockManager { get; }

        internal Transaction Transaction { get; }

        internal Stack<LatchReleaserEntry> Latches { get; }

        internal LowLevelTransactionJournal Journal { get; }

        internal Dictionary<PagePosition, TempBufferCache> Modifies { get; }

        internal Dictionary<object, LatchReleaserEntry> LatchTargets { get; }

        public LowLevelTransaction(long id, BufferPool buffers)
        {
            Id = id;
            Journal = new LowLevelTransactionJournal();
            Latches = new Stack<LatchReleaserEntry>();
            Buffers = buffers ?? throw new ArgumentNullException(nameof(buffers));
            Modifies = new Dictionary<PagePosition, TempBufferCache>();
            LatchTargets = new Dictionary<object, LatchReleaserEntry>();
        }

        public Page GetPage(int id, long number)
        {
            return GetPage(new PagePosition(id, number));
        }

        public Page GetPage(PagePosition pos)
        {
            var temporary = GetModifiedPage(pos);
            if (temporary != null)
            {
                return temporary;
            }

            var buffer = Buffers.GetBuffer(pos);
            if (buffer == null)
            {
                return null;
            }

            return GetPage(buffer);
        }

        public Page GetPage(BufferEntry buffer)
        {
            var temporary = GetModifiedPage(buffer.Position);
            if (temporary != null)
            {
                return temporary;
            }

            if (!LatchTargets.ContainsKey(buffer.Page.Position))
            {
                AddLatch(buffer.Latch.EnterRead());
            }

            return buffer.Page;
        }

        public Page ModifyPage(PagePosition pos)
        {
            var temporary = GetModifiedPage(pos);
            if (temporary != null)
            {
                return temporary;
            }

            var buffer = Buffers.GetBuffer(pos);
            if (buffer == null)
            {
                return null;
            }

            if (LatchTargets.TryGetValue(buffer.Page.Position, out var latch))
            {
                if (latch.Flags == LatchFlags.Read)
                {
                    throw new InvalidOperationException($"buffer's read latch has been hold {buffer.Page.Position}");
                }
            }
            else
            {
                AddLatch(buffer.Latch.EnterWrite());
            }

            temporary = buffer.Page.CreateCopy();
            Modifies[buffer.Position] = new TempBufferCache(buffer, temporary);

            return temporary;
        }

        public Page ModifyPage(BufferEntry buffer)
        {
            if (Modifies.TryGetValue(buffer.Position, out var cache))
            {
                return cache.Temporary;
            }

            if (LatchTargets.TryGetValue(buffer.Position, out var latch))
            {
                if (latch.Flags == LatchFlags.Read)
                {
                    throw new InvalidOperationException($"buffer's read latch has been hold {buffer.Page.Position}");
                }
            }
            else
            {
                AddLatch(buffer.Latch.EnterWrite());
            }

            cache = new TempBufferCache(buffer, buffer.Page.CreateCopy());
            Modifies[buffer.Position] = cache;

            return cache.Temporary;
        }

        protected Page GetModifiedPage(PagePosition pos)
        {
            if (Modifies.TryGetValue(pos, out var cache))
            {
                return cache.Temporary;
            }

            return null;
        }

        public void AddLatch(LatchReleaserEntry entry)
        {
            if (LatchTargets.TryGetValue(entry.Latch.Target, out var old))
            {
                throw new InvalidOperationException($"latch at target:{old.Latch.Target} has already exists!");
            }

            Latches.Push(entry);
            LatchTargets.Add(entry.Latch.Target, entry);
        }

        public LowLevelTransaction StartNew()
        {
            return new LowLevelTransaction(Id, Buffers);
        }

        public void Reset()
        {
            Journal.Clear();
            Modifies.Clear();
            Latches.Clear();
            LatchTargets.Clear();
        }

        public void Commit()
        {
            CopyTempToPages();
            ReleaseLatches();
            Reset();
        }

        public void Dispose()
        {
            Commit();
        }

        private void CopyTempToPages()
        {
            foreach (var item in Modifies)
            {
                var page = item.Value.Buffer.Page;
                var temp = item.Value.Temporary;

                temp.CopyTo(page);
            }
        }

        private void ReleaseLatches()
        {
            lock (Buffers.SyncRoot)
            {
                while (Latches.Count != 0)
                {
                    var latch = Latches.Pop();
                    if (latch.Latch.Target is BufferEntry entry)
                    {
                        entry.Count--;
                    }

                    latch.Dispose();
                }
            }
        }
    }

    internal struct TempBufferCache
    {
        public Page Temporary;

        public BufferEntry Buffer;

        public TempBufferCache(BufferEntry buffer, Page temporary)
        {
            Buffer = buffer;
            Temporary = temporary;
        }
    }
}
