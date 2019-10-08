using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;
using Vicuna.Storage.Collections;

namespace Vicuna.Engine.Transactions
{
    public partial class LowLevelTransaction : IDisposable
    {
        public long Id { get; }

        public bool Modified { get; set; }

        public BufferPool Buffers { get; }

        public FastList<byte> Logger { get; }

        public LockManager LockManager { get; }

        public PageManager PageManager { get; }

        public Transaction Transaction { get; }

        public HashSet<PagePosition> Modifies { get; }

        public Dictionary<object, LatchScope> LatchLocks { get; }

        public bool LogEnable { get; }

        public LowLevelTransaction(long id, BufferPool buffers)
        {
            Id = id;
            Buffers = buffers;
            Logger = new FastList<byte>();
            LatchLocks = new Dictionary<object, LatchScope>();
            Transaction = EngineEnviorment.Transaction;
            LockManager = EngineEnviorment.LockManager;
        }

        #region Latch

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LatchScope PopLatch(BufferEntry buffer)
        {
            if (LatchLocks.Remove(buffer.Position, out var latchLock))
            {
                return latchLock;
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PushLatch(PagePosition page, LatchScope scope)
        {
            if (!LatchLocks.TryAdd(page, scope))
            {
                throw new InvalidOperationException($"latch for {page} has already exists!");
            }
        }

        public void ExitLatch(PagePosition page)
        {
            if (LatchLocks.Remove(page, out var latchLock))
            {
                latchLock.Dispose();
            }
        }

        public void ExitLatch(BufferEntry buffer)
        {
            ExitLatch(buffer.Position);
        }

        public void ExitLatch(int fileId, long pageNumber)
        {
            ExitLatch(new PagePosition(fileId, pageNumber));
        }

        public Page EnterRead(int fileId, long pageNumber)
        {
            return EnterLatch(fileId, pageNumber, LatchFlags.Read);
        }

        public Page EnterRead(PagePosition page)
        {
            return EnterLatch(page, LatchFlags.Read);
        }

        public Page EnterRead(BufferEntry buffer)
        {
            return EnterLatch(buffer, LatchFlags.Read);
        }

        public Page EnterWrite(int fileId, long pageNumber)
        {
            return EnterLatch(fileId, pageNumber, LatchFlags.Write);
        }

        public Page EnterWrite(PagePosition page)
        {
            return EnterLatch(page, LatchFlags.Write);
        }

        public Page EnterWrite(BufferEntry buffer)
        {
            return EnterLatch(buffer, LatchFlags.Write);
        }

        public Page EnterLatch(int fileId, long pageNumber, LatchFlags flags)
        {
            return EnterLatch(new PagePosition(fileId, pageNumber), flags);
        }

        public Page EnterLatch(PagePosition page, LatchFlags flags)
        {
            if (LatchLocks.TryGetValue(page, out var latchLock))
            {
                if (flags != latchLock.Flags)
                {
                    throw new InvalidOperationException($"has hold a {flags} lactch of the buffer:{page}!");
                }

                return (latchLock.Latch.Target as BufferEntry)?.Page;
            }

            var buffer = Buffers.GetEntry(page);
            if (buffer == null)
            {
                throw new NullReferenceException(nameof(buffer));
            }

            switch (flags)
            {
                case LatchFlags.Read:
                    LatchLocks[page] = buffer.Latch.EnterReadScope();
                    break;
                case LatchFlags.Write:
                    LatchLocks[page] = buffer.Latch.EnterWriteScope();
                    break;
                case LatchFlags.RWRead:
                    LatchLocks[page] = buffer.Latch.EnterReadWriteScope();
                    break;
            }

            return buffer.Page;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page EnterLatch(BufferEntry buffer, LatchFlags flags)
        {
            if (LatchLocks.TryGetValue(buffer.Position, out var latchLock))
            {
                if (flags != latchLock.Flags)
                {
                    throw new InvalidOperationException($"has hold a {flags} lactch of the buffer:{buffer.Position}!");
                }

                return buffer.Page;
            }

            switch (flags)
            {
                case LatchFlags.Read:
                    LatchLocks[buffer.Position] = buffer.Latch.EnterReadScope();
                    break;
                case LatchFlags.RWRead:
                    LatchLocks[buffer.Position] = buffer.Latch.EnterReadWriteScope();
                    break;
                case LatchFlags.Write:
                    LatchLocks[buffer.Position] = buffer.Latch.EnterWriteScope();
                    break;
            }

            return buffer.Page;
        }

        public bool CheckLatch(BufferEntry buffer, LatchFlags flags)
        {
            if (!LatchLocks.TryGetValue(buffer.Position, out var latch))
            {
                return false;
            }

            switch (flags)
            {
                case LatchFlags.Write:
                    return latch.Flags == LatchFlags.Write || latch.Flags == LatchFlags.RWWrite;
                default:
                    return true;
            }
        }

        #endregion

        public PagePosition AllocatePage(int fileId)
        {
            return new PagePosition(fileId, System.Threading.Interlocked.Increment(ref count));
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

        public void Commit()
        {
            if (Modified)
            {
                foreach (var item in Modifies)
                {
                    Buffers.AddFlushEntry(item);
                }
            }

            ReleaseResources();
            LatchLocks.Clear();
        }

        public void Dispose()
        {
            Commit();
        }

        private void ReleaseResources()
        {
            lock (Buffers.SyncRoot)
            {
                foreach (var item in LatchLocks)
                {
                    var latch = item.Value;
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