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
        internal object SyncRoot { get; } = new object();

        internal Dictionary<Index, LinkedList<LockEntry>> TabLocks { get; }

        internal Dictionary<PagePosition, LinkedList<LockEntry>> RecLocks { get; set; }

        public LockManager()
        {
            TabLocks = new Dictionary<Index, LinkedList<LockEntry>>();
            RecLocks = new Dictionary<PagePosition, LinkedList<LockEntry>>();
        }

        public DBOperationFlags LockTable(Transaction tx, Index index, LockFlags flags)
        {
            return DBOperationFlags.Error;
        }

        public DBOperationFlags LockRec(ref LockRequest req)
        {
            return LockRec(ref req, out var _);
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

            if (IsOthersHeldConflictRecLock(ref req))
            {
                req.Flags |= LockFlags.Waiting;
                return CreateRecLockThenWaitting(ref req, out entry);
            }

            entry = ReuseExistsRecLock(ref req) ?? CreateRecLock(ref req);
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
            if (locks == null || locks.Count == 0)
            {
                entry = null;
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.IsWaiting ||
                    lockEntry.Transaction != req.Transaction ||
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
        private bool IsHeldTableLock(ref LockRequest req)
        {
            var locks = TabLocks.GetValueOrDefault(req.Index);
            if (locks == null || locks.Count == 0)
            {
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.IsWaiting || lockEntry.Transaction != req.Transaction)
                {
                    continue;
                }

                if (lockEntry.IsExclusive || !req.Flags.IsExclusive())
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// is other's transaction has held the recrod's lock and conflict with the req-lock
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private bool IsOthersHeldConflictRecLock(ref LockRequest req)
        {
            var locks = RecLocks.GetValueOrDefault(req.Position);
            if (locks == null || locks.Count == 0)
            {
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.IsWaiting ||
                    lockEntry.Transaction == req.Transaction ||
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
        private bool IsOthersHeldConflictTableLock(ref LockRequest req)
        {
            var locks = TabLocks.GetValueOrDefault(req.Index);
            if (locks == null || locks.Count == 0)
            {
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.IsWaiting ||
                    lockEntry.Transaction == req.Transaction)
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
            if (!RecLocks.TryGetValue(req.Position, out var locks))
            {
                RecLocks[req.Position] = locks = new LinkedList<LockEntry>();
            }

            var trxLocks = req.Transaction.RecLocks;
            var entry = new LockEntry(req.Position, req.Flags, req.RecordCount);
            if (entry.IsWaiting)
            {
                req.Transaction.WaitLock = entry;
            }

            locks.AddLast(entry);
            trxLocks.AddLast(entry);

            entry.GNode = locks.Last;
            entry.TNode = trxLocks.Last;
            entry.Index = req.Index;
            entry.Transaction = req.Transaction;
            entry.Thread = Thread.CurrentThread.ManagedThreadId;
            entry.SetBit(req.RecordSlot, 1);

            return entry;
        }

        /// <summary>
        /// reuse the current transaction's created rec-lock
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private LockEntry ReuseExistsRecLock(ref LockRequest req)
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

            if (entry == null)
            {
                return null;
            }

            if (entry.GetBit(req.RecordSlot) == 0)
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
        private DBOperationFlags CreateRecLockThenWaitting(ref LockRequest req, out LockEntry entry)
        {
            var recEntry = CreateRecLock(ref req);
            if (recEntry.IsWaiting == false)
            {
                recEntry.Flags |= LockFlags.Waiting;
            }

            req.Transaction.WaitLock = recEntry;
            req.Transaction.State = TransactionState.Waitting;

            //check dead-lock
            if (IsRecCausedDeadLock(recEntry))
            {
                recEntry.Flags &= ~LockFlags.Waiting;
                recEntry.SetBit(req.RecordSlot, 0);
                recEntry.Transaction.WaitLock = null;
                entry = recEntry;
                return DBOperationFlags.DeadLock;
            }

            entry = recEntry;
            req.Transaction.WaitEvent.Reset();

            return DBOperationFlags.Waitting;
        }

        /// <summary>
        /// check if the rec-lock caused dead-lock
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        private bool IsRecCausedDeadLock(LockEntry entry)
        {
            if (!entry.IsWaiting)
            {
                throw new InvalidOperationException($"err api invoke!");
            }

            if (entry.Transaction.RecLocks.Count == 1)
            {
                return false;
            }

            foreach (var trx in EngineEnviorment.Transactions.Values)
            {
                trx.DeadFlags = 0;
            }

            return IsRecCausedDeadLock(entry.Transaction, entry);
        }

        /// <summary>
        /// check if the rec-lock caused dead-lock
        /// </summary>
        /// <param name="initTx"></param>
        /// <returns></returns>
        private bool IsRecCausedDeadLock(Transaction initTx, LockEntry checkEntry)
        {
            var entry = checkEntry;
            if (entry.Transaction.DeadFlags == 1)
            {
                return true;
            }

            var lockBit = entry.GetFirstMarkedIndex();
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
                    prevEntry.GetBit(lockBit) == 0)
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
                        IsRecCausedDeadLock(initTx, prevEntry.Transaction.WaitLock))
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