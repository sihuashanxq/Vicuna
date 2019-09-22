﻿using System;
using System.Collections.Generic;
using System.IO;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Storages;
using Vicuna.Storage.Collections;

namespace Vicuna.Engine.Transactions
{
    public partial class LowLevelTransaction : IDisposable
    {
        public long Id { get; }

        public bool Modified { get; set; }

        internal BufferPool Buffers { get; }

        internal FastList<byte> Logger { get; }

        internal PageManager PageManager { get; }

        internal Transaction Transaction { get; }

        internal Stack<LatchScope> Latches { get; }

        internal Dictionary<PagePosition, Page> Modifies { get; }

        internal Dictionary<object, LatchScope> LatchMaps { get; }

        public LowLevelTransaction(long id, BufferPool buffers)
        {
            Id = id;
            Buffers = buffers;
            Logger = new FastList<byte>();
            Latches = new Stack<LatchScope>();
            Modifies = new Dictionary<PagePosition, Page>();
            LatchMaps = new Dictionary<object, LatchScope>();
        }

        public Page GetPage(int fileId, long pageNumber)
        {
            return GetPage(new PagePosition(fileId, pageNumber));
        }

        public Page GetPage(PagePosition pos)
        {
            if (Modifies.TryGetValue(pos, out var cache))
            {
                return cache;
            }

            if (LatchMaps.TryGetValue(pos, out var entry))
            {
                return ((BufferEntry)entry?.Latch?.Target)?.Page;
            }

            var buffer = Buffers.GetEntry(pos);
            if (buffer == null)
            {
                return null;
            }

            return GetPage(buffer);
        }

        public Page GetPage(BufferEntry buffer)
        {
            if (Modifies.TryGetValue(buffer.Page.Position, out var cache))
            {
                return cache;
            }

            if (!LatchMaps.ContainsKey(buffer.Page.Position))
            {
                AddLatchScope(buffer.Latch.EnterReadScope());
            }

            return buffer.Page;
        }

        public bool HasBufferLatch(BufferEntry buffer, LatchFlags flags)
        {
            if (!LatchMaps.TryGetValue(buffer.Page.Position, out var latch))
            {
                return false;
            }

            switch (flags)
            {
                case LatchFlags.Read:
                    return latch.Flags == LatchFlags.Read || latch.Flags == LatchFlags.Write;
                case LatchFlags.Write:
                    return latch.Flags == LatchFlags.Write;
                default:
                    return true;
            }
        }

        public Page ModifyPage(int fileId, long pageNumber)
        {
            return ModifyPage(new PagePosition(fileId, pageNumber));
        }

        public Page ModifyPage(PagePosition pos)
        {
            if (!Modifies.TryGetValue(pos, out var entry))
            {
                var buffer = Buffers.GetEntry(pos);
                if (buffer == null)
                {
                    return null;
                }

                AddLatchScope(buffer.Latch.EnterWriteScope());
                Modifies[buffer.Position] = buffer.Page;

                return buffer.Page;
            }

            return entry;
        }

        public Page ModifyPage(BufferEntry buffer)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            if (!Modifies.TryGetValue(buffer.Page.Position, out var entry))
            {
                AddLatchScope(buffer.Latch.EnterWriteScope());
                Modifies[buffer.Position] = buffer.Page;

                return buffer.Page;
            }

            return entry;
        }

        public void FreePage(Page page)
        {

        }

        public PagePosition AllocatePage(int fileId)
        {
            return new PagePosition(fileId, ++count);
        }

        private static int count = 1;

        public PagePosition[] AllocatePage(int fileId, uint count)
        {
            return new PagePosition[count];
        }

        public LowLevelTransaction StartNew()
        {
            return new LowLevelTransaction(Id, Buffers);
        }

        protected void AddLatchScope(LatchScope scope)
        {
            if (LatchMaps.TryGetValue(scope.Latch.Target, out var old))
            {
                if (old.Flags != scope.Flags)
                {
                    throw new InvalidOperationException($"latch at target:{old.Latch.Target} has already exists!");
                }

                return;
            }

            Latches.Push(scope);
            LatchMaps.Add(scope.Latch.Target, scope);
        }

        public void Commit()
        {
            ReleaseResources();

            Modifies.Clear();
            Latches.Clear();
            LatchMaps.Clear();
        }

        public void Dispose()
        {
            Commit();
        }

        private void ReleaseResources()
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
}