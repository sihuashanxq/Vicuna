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

        internal PageBufferPool Buffers { get; }

        internal LowLevelTransactionJournal Journal { get; }

        internal Stack<LatchReleaserEntry> Latches { get; }

        internal Dictionary<object, LatchReleaserEntry> LatchTargets { get; }

        internal Dictionary<PagePosition, TempBufferCache> Modifies { get; }

        public LowLevelTransaction(long id, PageBufferPool buffers)
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

            var buffer = Buffers.GetEntry(pos);
            if (buffer == null)
            {
                return null;
            }

            return GetPage(buffer);
        }

        public Page GetPage(PageBufferEntry buffer)
        {
            var temporary = GetModifiedPage(buffer.Position);
            if (temporary != null)
            {
                return null;
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

            var buffer = Buffers.GetEntry(pos);
            if (buffer == null)
            {
                return null;
            }

            buffer.Lock.EnterWriteLock();
            temporary = buffer.Page.CreateCopy();

            Modifies[buffer.Position] = new TempBufferCache(buffer, temporary);
            AddLockReleaser(ReadWriteLockType.Write, buffer.Lock);

            return temporary;
        }

        public Page ModifyPage(PageBufferEntry buffer)
        {
            if (!Modifies.TryGetValue(buffer.Position, out var cache))
            {
                buffer.Lock.EnterWriteLock();
                cache = new TempBufferCache(buffer, buffer.Page.CreateCopy());

                Modifies[buffer.Position] = cache;
                AddLockReleaser(ReadWriteLockType.Write, buffer.Lock);
            }

            return cache.Temporary;
        }

        public Page GetModifiedPage(PagePosition pos)
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
                    if (latch.Latch.Target is PageBufferEntry entry)
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

        public PageBufferEntry Buffer;

        public TempBufferCache(PageBufferEntry buffer, Page temporary)
        {
            Buffer = buffer;
            Temporary = temporary;
        }
    }
}
