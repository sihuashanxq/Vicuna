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

        internal Dictionary<PagePosition, LinkedList<LockEntry>> Locks { get; }

        public DBOperationFlags Lock(Transaction tx, Index index, LockFlags flags)
        {
            return DBOperationFlags.Error;
        }

        public DBOperationFlags Lock(Transaction tx, Index index, Span<byte> clusterKey, LockFlags flags)
        {
            return DBOperationFlags.Error;
        }

        public DBOperationFlags Lock(ref LockRequest req)
        {
            if (LockFast(ref req, out var entry))
            {
                return entry.IsWaiting ? DBOperationFlags.Waitting : DBOperationFlags.Success;
            }

            return LockSlow(ref req, out entry);
        }

        public bool LockFast(ref LockRequest req, out LockEntry entry)
        {
            var locks = Locks.GetValueOrDefault(req.Position);
            if (locks == null || locks.Count == 0)
            {
                entry = CreateEntry(ref req);
                return true;
            }

            entry = null;
            return false;
        }

        public DBOperationFlags LockSlow(ref LockRequest req, out LockEntry entry)
        {
            if (IsHeldLock(ref req))
            {
                entry = null;
                return DBOperationFlags.Success;
            }

            if (IsOthersHeldLock(ref req))
            {
                entry = CreateEntryForWaitting(ref req);
            }
            else
            {
                entry = CreateEntry(ref req);
            }

            if (entry == null)
            {
                return DBOperationFlags.Error;
            }

            return entry.IsWaiting ? DBOperationFlags.Waitting : DBOperationFlags.Success;
        }

        private bool IsHeldLock(ref LockRequest req)
        {
            var locks = Locks.GetValueOrDefault(req.Position);
            if (locks == null)
            {
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.IsExclusive &&
                    lockEntry.Transaction == req.Transaction &&
                    lockEntry.GetBit(req.RecordSlot) == 1)
                {
                    return true;
                }
            }

            return false;
        }

        private bool IsOthersHeldLock(ref LockRequest req)
        {
            var locks = Locks.GetValueOrDefault(req.Position);
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

                return true;
            }

            return false;
        }

        private LockEntry CreateEntry(ref LockRequest req)
        {
            if (!Locks.TryGetValue(req.Position, out var locks))
            {
                Locks[req.Position] = locks = new LinkedList<LockEntry>();
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

        private LockEntry CreateEntryForWaitting(ref LockRequest req)
        {
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindLockEntry(PagePosition pos)
        {
            return Locks.TryGetValue(pos, out var list) ? list.First.Value : null;
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
