using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Transactions;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Data.Trees;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Locking
{
    public class LockManager
    {
        internal object SyncRoot { get; } = new object();

        internal Dictionary<Index, LinkedList<LockEntry>> TableLocks { get; }

        internal Dictionary<PagePosition, LinkedList<LockEntry>> RecLocks { get; }

        public DBOperationFlags LockTable(Transaction tx, Index index, LockFlags flags)
        {
            return DBOperationFlags.Error;
        }

        public DBOperationFlags LockRec(Transaction tx, Index index, Span<byte> clusterKey, LockFlags flags)
        {
            return DBOperationFlags.Error;
        }

        public DBOperationFlags LockRec(ref LockRequest req)
        {
            if (LockRecFast(ref req, out var entry))
            {
                return entry.IsWaiting ? DBOperationFlags.Waitting : DBOperationFlags.Success;
            }

            return LockRecSlow(ref req, out entry);
        }

        /// <summary>
        /// lock record fastly
        /// </summary>
        /// <param name="req"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        public bool LockRecFast(ref LockRequest req, out LockEntry entry)
        {
            var locks = RecLocks.GetValueOrDefault(req.Position);
            if (locks == null || locks.Count == 0)
            {
                entry = CreateRecEntry(ref req);
                return true;
            }

            entry = null;
            return false;
        }

        /// <summary>
        /// lock record slowly
        /// </summary>
        /// <param name="req"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        public DBOperationFlags LockRecSlow(ref LockRequest req, out LockEntry entry)
        {
            if (IsHeldRecLock(ref req))
            {
                entry = null;
                return DBOperationFlags.Success;
            }

            if (IsOthersHeldConflictRecLock(ref req))
            {
                req.Flags |= LockFlags.Waiting;
                entry = CreateRecEntryForWaitting(ref req);
            }
            else
            {
                entry = CreateRecEntry(ref req);
            }

            if (entry == null)
            {
                return DBOperationFlags.Error;
            }

            return entry.IsWaiting ? DBOperationFlags.Waitting : DBOperationFlags.Success;
        }

        /// <summary>
        /// check current transaction has held the record's lock >=req-lock-level and not been in wait state
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private bool IsHeldRecLock(ref LockRequest req)
        {
            var locks = RecLocks.GetValueOrDefault(req.Position);
            if (locks == null)
            {
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
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// check current transaction has held the table's lock >=req-lock-level and not been in wait state
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        private bool IsHeldTableLock(ref LockRequest req)
        {
            var locks = TableLocks.GetValueOrDefault(req.Index);
            if (locks == null)
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
            if (locks == null)
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
            var locks = TableLocks.GetValueOrDefault(req.Index);
            if (locks == null)
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

        private LockEntry CreateRecEntry(ref LockRequest req)
        {
            if (!RecLocks.TryGetValue(req.Position, out var locks))
            {
                RecLocks[req.Position] = locks = new LinkedList<LockEntry>();
            }

            var trxLocks = req.Transaction.Locks;
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
            entry.SetBit(req.RecordCount, 1);

            return entry;
        }

        private LockEntry CreateRecEntryForWaitting(ref LockRequest req)
        {
            req.Transaction.WaitLock = null;
            req.Transaction.WaitEvent.Reset();
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindLockEntry(PagePosition pos)
        {
            return RecLocks.TryGetValue(pos, out var list) ? list.First.Value : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindNextLockEntry(LockEntry entry)
        {
            return entry?.GNode?.Next?.Value;
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

            if (entry.IsTable && !entry.IsExclusive || entry.IsTable && !other.IsExclusive)
            {
                return false;
            }

            return entry.IsExclusive || other.IsExclusive;
        }
    }
}
