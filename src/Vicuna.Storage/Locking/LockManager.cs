using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Data.Trees;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Locking
{
    public class LockManager
    {
        public object SyncRoot { get; } = new object();

        public Dictionary<Index, LinkedList<LockEntry>> TabLocks { get; }

        public Dictionary<PagePosition, LinkedList<LockEntry>> RecLocks { get; set; }

        public LockManager()
        {
            TabLocks = new Dictionary<Index, LinkedList<LockEntry>>();
            RecLocks = new Dictionary<PagePosition, LinkedList<LockEntry>>();
        }

        /// <summary>
        /// lock a table
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        public DBOperationFlags LockTab(ref LockRequest req)
        {
            if (req.Flags.IsDocument())
            {
                throw new InvalidOperationException("err api invoke!");
            }

            return LockTab(ref req, out var _);
        }

        /// <summary>
        /// lock a record
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        public DBOperationFlags LockRec(ref LockRequest req)
        {
            if (req.Flags.IsTable())
            {
                throw new InvalidOperationException("err api invoke!");
            }

            return LockRec(ref req, out var _);
        }

        public void MergeRecLock(PagePosition left, PagePosition right)
        {
            
        }

        public void SplitRecLock(PagePosition from, PagePosition to, int mid)
        {
            var removes = new List<LockEntry>();
            var fromEntry = FindFirstRecLockEntry(from);
            var list = new LinkedList<LockEntry>();

            while (fromEntry != null)
            {
                var tx = fromEntry.Transaction;
                var toEntry = SplitRecLock(fromEntry, to, mid);
                if (toEntry == null)
                {
                    fromEntry = FindNextRecLockEntry(fromEntry);
                    continue;
                }

                toEntry.TNode = tx.Locks.AddAfter(fromEntry.TNode, toEntry);
                toEntry.GNode = list.AddLast(toEntry);

                if (fromEntry.IsEmpty)
                {
                    removes.Add(fromEntry);
                }

                if (fromEntry.IsWaiting && fromEntry.IsEmpty)
                {
                    tx.WaitLock = toEntry;
                }

                fromEntry = FindNextRecLockEntry(fromEntry);
            }

            foreach (var entry in removes)
            {
                entry.TNode.List.Remove(entry.TNode);
                entry.GNode.List.Remove(entry.GNode);
            }

            RecLocks[to] = list;
        }

        private unsafe LockEntry SplitRecLock(LockEntry fromEntry, PagePosition to, int mid)
        {
            if (fromEntry == null)
            {
                return null;
            }

            var count = fromEntry.Count - mid / 8 + 8;
            var buffer = stackalloc byte[count];

            fromEntry.CopyBitsTo(mid, buffer);
            fromEntry.ResetBits(mid);

            for (var i = 0; i < count; i++)
            {
                if (buffer[i] == 0)
                {
                    continue;
                }

                var toEntry = new LockEntry()
                {
                    Bits = new byte[count],
                    Page = to,
                    Flags = fromEntry.Flags,
                    Index = fromEntry.Index,
                    Thread = fromEntry.Thread,
                    Transaction = fromEntry.Transaction
                };

                Unsafe.CopyBlock(ref toEntry.Bits[0], ref * buffer, (uint) count);

                return toEntry;
            }

            return null;
        }

        /// <summary>
        /// lock a record
        /// </summary>
        /// <param name="req"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        public DBOperationFlags LockRec(ref LockRequest req, out LockEntry entry)
        {
            var locks = RecLocks.GetValueOrDefault(req.Position);
            if (locks == null || locks.Count == 0)
            {
                entry = CreateRecLock(ref req);
                return DBOperationFlags.Success;
            }

            if (IsHeldRecLock(ref req, out entry))
            {
                return DBOperationFlags.Success;
            }

            if (IsOthersHeldOrWaitConflictRecLock(ref req))
            {
                req.Flags |= LockFlags.Waiting;
                return CreateRecLockForWait(ref req, out entry);
            }

            entry = GetCanReuseRecLock(ref req) ?? CreateRecLock(ref req);
            return DBOperationFlags.Success;
        }

        /// <summary>
        /// lock a table
        /// </summary>
        /// <param name="req"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        public DBOperationFlags LockTab(ref LockRequest req, out LockEntry entry)
        {
            var locks = TabLocks.GetValueOrDefault(req.Index);
            if (locks == null || locks.Count == 0)
            {
                entry = CreateTabLock(ref req);
                return DBOperationFlags.Success;
            }

            if (IsHeldTabLock(ref req, out entry))
            {
                return DBOperationFlags.Success;
            }

            if (IsOthersHeldOrWaitConflictTabLock(ref req))
            {
                req.Flags |= LockFlags.Waiting;
                return CreateTabLockForWait(ref req, out entry);
            }

            entry = CreateTabLock(ref req);
            return DBOperationFlags.Success;
        }

        /// <summary>
        /// check current transaction has held the record's lock >=req-lock-level and not been in wait state
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private bool IsHeldRecLock(ref LockRequest req, out LockEntry entry)
        {
            var locks = RecLocks.GetValueOrDefault(req.Position);
            if (locks == null)
            {
                entry = null;
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.Transaction != req.Transaction ||
                    lockEntry.GetBit(req.RecordSlot) == 0)
                {
                    continue;
                }

                if (lockEntry.IsExclusive || !req.Flags.IsExclusive())
                {
                    entry = lockEntry;
                    return true;
                }
            }

            entry = null;
            return false;
        }

        /// <summary>
        /// check current transaction has held the table's lock >=req-lock-level and not been in wait state
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private bool IsHeldTabLock(ref LockRequest req, out LockEntry entry)
        {
            var locks = TabLocks.GetValueOrDefault(req.Index);
            if (locks == null)
            {
                entry = null;
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.Transaction != req.Transaction)
                {
                    continue;
                }

                if (lockEntry.IsExclusive || !req.Flags.IsExclusive())
                {
                    entry = lockEntry;
                    return true;
                }
            }

            entry = null;
            return false;
        }

        /// <summary>
        /// is other's transaction has held the recrod's lock and conflict with the req-lock
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private bool IsOthersHeldOrWaitConflictRecLock(ref LockRequest req)
        {
            var locks = RecLocks.GetValueOrDefault(req.Position);
            if (locks == null)
            {
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.Transaction == req.Transaction ||
                    lockEntry.GetBit(req.RecordSlot) == 0)
                {
                    continue;
                }

                if (lockEntry.IsExclusive || req.Flags.IsExclusive())
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// is other's transaction has held the table's lock and conflict with the req-lock
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private bool IsOthersHeldOrWaitConflictTabLock(ref LockRequest req)
        {
            var locks = TabLocks.GetValueOrDefault(req.Index);
            if (locks == null)
            {
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.Transaction == req.Transaction)
                {
                    continue;
                }

                if (lockEntry.IsExclusive || req.Flags.IsExclusive())
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// create a new rec-lock
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private LockEntry CreateRecLock(ref LockRequest req)
        {
            if (!RecLocks.TryGetValue(req.Position, out var list))
            {
                list = new LinkedList<LockEntry>();
                RecLocks[req.Position] = list;;
            }

            var tx = req.Transaction;
            var entry = new LockEntry(req.Position, req.Flags, req.RecordCount);
            if (entry.IsWaiting)
            {
                tx.WaitLock = entry;
            }

            list.AddLast(entry);
            tx.Locks.AddLast(entry);

            entry.GNode = list.Last;
            entry.TNode = tx.Locks.Last;
            entry.Index = req.Index;
            entry.Transaction = req.Transaction;
            entry.Thread = Thread.CurrentThread.ManagedThreadId;
            entry.SetBit(req.RecordSlot, 1);

            return entry;
        }

        /// <summary>
        /// create a new table-lock
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private LockEntry CreateTabLock(ref LockRequest req)
        {
            if (!TabLocks.TryGetValue(req.Index, out var list))
            {
                list = new LinkedList<LockEntry>();
                TabLocks[req.Index] = list;;
            }

            var tx = req.Transaction;
            var entry = new LockEntry(req.Position, req.Flags, req.RecordCount);
            if (entry.IsWaiting)
            {
                tx.WaitLock = entry;
            }

            list.AddLast(entry);
            tx.Locks.AddLast(entry);

            entry.GNode = list.Last;
            entry.TNode = tx.Locks.Last;
            entry.Index = req.Index;
            entry.Transaction = req.Transaction;
            entry.Thread = Thread.CurrentThread.ManagedThreadId;

            return entry;
        }

        /// <summary>
        /// reuse the current transaction's created rec-lock
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private LockEntry GetCanReuseRecLock(ref LockRequest req)
        {
            var entry = default(LockEntry);
            var locks = RecLocks.GetValueOrDefault(req.Position);
            if (locks == null)
            {
                return null;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.Transaction == req.Transaction &&
                    lockEntry.IsExclusive == req.Flags.IsExclusive())
                {
                    entry = lockEntry;
                    break;
                }
            }

            if (entry != null)
            {
                entry.SetBit(req.RecordSlot, 1);
            }

            return entry;
        }

        /// <summary>
        /// create new rec lock then waitting
        /// </summary>
        /// <param name="req"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        private DBOperationFlags CreateRecLockForWait(ref LockRequest req, out LockEntry entry)
        {
            var recEntry = CreateRecLock(ref req);
            if (recEntry.IsWaiting == false)
            {
                recEntry.Flags |= LockFlags.Waiting;
            }

            entry = recEntry;
            entry.Transaction.WaitLock = recEntry;
            entry.Transaction.State = TransactionState.Waitting;

            //check dead-lock
            if (IsCausedDeadLock(recEntry))
            {
                entry.Flags &= ~LockFlags.Waiting;
                entry.SetBit(req.RecordSlot, 0);
                entry.Transaction.WaitLock = null;
                return DBOperationFlags.DeadLock;
            }

            entry.Transaction.WaitEvent.Reset();
            return DBOperationFlags.Waitting;
        }

        /// <summary>
        /// create new rec lock then waitting
        /// </summary>
        /// <param name="req"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        private DBOperationFlags CreateTabLockForWait(ref LockRequest req, out LockEntry entry)
        {
            var tabEntry = CreateTabLock(ref req);
            if (tabEntry.IsWaiting == false)
            {
                tabEntry.Flags |= LockFlags.Waiting;
            }

            entry = tabEntry;
            entry.Transaction.WaitLock = tabEntry;
            entry.Transaction.State = TransactionState.Waitting;

            //check dead-lock
            if (IsCausedDeadLock(tabEntry))
            {
                entry.Flags &= ~LockFlags.Waiting;
                entry.Transaction.WaitLock = null;
                return DBOperationFlags.DeadLock;
            }

            entry.Transaction.WaitEvent.Reset();
            return DBOperationFlags.Waitting;
        }

        /// <summary>
        /// check if the lock caused dead-lock
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        private bool IsCausedDeadLock(LockEntry entry)
        {
            if (!entry.IsWaiting)
            {
                throw new InvalidOperationException($"err api invoke!");
            }

            if (entry.Transaction.Locks.Count == 1)
            {
                return false;
            }

            foreach (var trx in EngineEnviorment.Transactions.Values)
            {
                trx.DeadFlags = 0;
            }

            return IsCausedDeadLock(entry.Transaction, entry);
        }

        /// <summary>
        /// check if the lock caused dead-lock
        /// </summary>
        /// <param name="initTx"></param>
        /// <returns></returns>
        private bool IsCausedDeadLock(Transaction initTx, LockEntry checkEntry)
        {
            var entry = checkEntry;
            if (entry.Transaction.DeadFlags == 1)
            {
                return true;
            }

            var lockBit = entry.IsTable ? 0 : entry.GetFirstBitIndex();
            if (lockBit < 0)
            {
                return false;
            }

            while (true)
            {
                var prevEntry = FindPrevRecLockEntry(entry);
                if (prevEntry == null)
                {
                    checkEntry.Transaction.DeadFlags = 1;
                    return false;
                }

                if (prevEntry.Transaction.State != TransactionState.Waitting ||
                    prevEntry.Transaction == checkEntry.Transaction ||
                    (!prevEntry.IsTable && prevEntry.GetBit(lockBit) == 0))
                {
                    entry = prevEntry;
                    continue;
                }

                if (checkEntry.IsExclusive || prevEntry.IsExclusive)
                {
                    if (prevEntry.Transaction == initTx)
                    {
                        return true;
                    }

                    if (prevEntry.Transaction.State == TransactionState.Waitting &&
                        IsCausedDeadLock(initTx, prevEntry.Transaction.WaitLock))
                    {
                        return true;
                    }
                }

                entry = prevEntry;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsLockConflict(LockEntry entry, LockEntry other)
        {
            if (entry == other || entry.Transaction == other.Transaction)
            {
                return false;
            }

            if (entry.IsTable && entry.IsExclusive || other.IsTable && other.IsExclusive)
            {
                return true;
            }
            else if (entry.IsTable || other.IsTable)
            {
                return true;
            }

            return entry.IsExclusive || other.IsExclusive;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MoveRecLockBits(PagePosition pos, int index)
        {
            var entry = FindFirstRecLockEntry(pos);

            while (entry != null)
            {
                entry.MoveBits(index);
                entry = FindNextRecLockEntry(entry);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ExtendRecLockCapacity(PagePosition pos, int count)
        {
            var entry = FindFirstRecLockEntry(pos);

            while (entry != null)
            {
                entry.ExtendCapacity(count);
                entry = FindNextRecLockEntry(entry);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindFirstRecLockEntry(PagePosition pos)
        {
            return RecLocks.TryGetValue(pos, out var list) ? list.First.Value : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindNextRecLockEntry(LockEntry entry)
        {
            return entry?.GNode?.Next?.Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindPrevRecLockEntry(LockEntry entry)
        {
            return entry?.GNode?.Previous?.Value;
        }
    }
}