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
        public DBOperationFlags Lock(ref LockRequest req)
        {
            return req.Flags.IsTable() ? LockTab(ref req, out var _) : LockRec(ref req, out var _);
        }

        /// <summary>
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        public DBOperationFlags UnLock(LockEntry entry)
        {
            if (entry == null)
            {
                return DBOperationFlags.Ok;
            }

            var list = entry.GNode.List;
            var next = FindNextLockEntry(entry);

            list.Remove(entry.GNode);

            return entry.IsTable ? UnLockTab(entry, next) : UnLockRec(entry, next);
        }

        /// <summary>
        /// un-lock-rec and try wake up from the next to the end
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="next"></param>
        /// <returns></returns>
        internal DBOperationFlags UnLockRec(LockEntry entry, LockEntry next)
        {
            while (next != null)
            {
                if (!next.IsWaiting)
                {
                    next = FindNextLockEntry(next);
                    continue;
                }

                var slot = next.GetFirstBitSlot();
                if (slot < 0 || entry.GetBit(slot) == 0)
                {
                    next = FindNextLockEntry(next);
                    continue;
                }

                if (IsOthersHeldOrWaitConflictRecLock(next.Transaction, next.Page, slot, next.Flags, next))
                {
                    next = FindNextLockEntry(next);
                    continue;
                }

                next.Flags &= ~LockFlags.Waiting;
                next.Transaction.WaitLock = null;
                next.Transaction.WaitEvent.Set();
                next.Transaction.State = TransactionState.Running;

                next = FindNextLockEntry(next);
            }

            return DBOperationFlags.Ok;
        }

        /// <summary>
        /// un-lock-tab and try wake up from the next to the end
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="next"></param>
        /// <returns></returns>
        internal DBOperationFlags UnLockTab(LockEntry entry, LockEntry next)
        {
            while (next != null)
            {
                if (!next.IsWaiting)
                {
                    next = FindNextLockEntry(next);
                    continue;
                }

                if (IsOthersHeldOrWaitConflictTabLock(next.Transaction, next.Index, next.Flags, next))
                {
                    next = FindNextLockEntry(next);
                    continue;
                }

                next.Flags |= ~LockFlags.Waiting;
                next.Transaction.WaitLock = null;
                next.Transaction.WaitEvent.Set();
                next.Transaction.State = TransactionState.Running;

                next = FindNextLockEntry(next);
            }

            return DBOperationFlags.Ok;
        }

        /// <summary>
        /// merge two page's rec-lock
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <param name="fromCount"></param>
        /// <param name="toCount"></param>
        public void MergeRecLock(PagePosition from, PagePosition to, int fromCount, int toCount)
        {
            var toEntry = FindFirstRecLockEntry(to);
            var fromEntry = FindFirstRecLockEntry(from);
            if (fromEntry == null && toEntry == null)
            {
                return;
            }

            var list = toEntry?.GNode?.List;
            if (list == null)
            {
                list = RecLocks[to] = new LinkedList<LockEntry>();
            }

            while (toEntry != null)
            {
                toEntry.ExtendHeadCapacity(fromCount);
                toEntry = FindNextLockEntry(toEntry);
            }

            while (fromEntry != null)
            {
                var entry = fromEntry;
                var tx = entry.Transaction;

                fromEntry.Page = to;
                fromEntry.ExtendTailCapacity(toCount);
                fromEntry = FindNextLockEntry(fromEntry);

                entry.GNode = list.AddLast(entry);
                entry.TNode = tx.Locks.AddLast(entry);
            }

            RecLocks.Remove(from);
        }

        /// <summary>
        /// split the from-page's rec-lock
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <param name="mid"></param>
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
                    fromEntry = FindNextLockEntry(fromEntry);
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

                fromEntry = FindNextLockEntry(fromEntry);
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

            var count = fromEntry.Count - (mid >> 3) + 8;
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

                Unsafe.CopyBlock(ref toEntry.Bits[0], ref *buffer, (uint)count);

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
                return DBOperationFlags.Ok;
            }

            if (IsHeldRecLock(ref req, out entry))
            {
                return DBOperationFlags.Ok;
            }

            if (IsOthersHeldOrWaitConflictRecLock(req.Transaction, req.Position, req.RecordSlot, req.Flags))
            {
                req.Flags |= LockFlags.Waiting;
                return CreateRecLockForWait(ref req, out entry);
            }

            entry = GetCanReuseRecLock(ref req) ?? CreateRecLock(ref req);
            return DBOperationFlags.Ok;
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
                return DBOperationFlags.Ok;
            }

            if (IsHeldTabLock(ref req, out entry))
            {
                return DBOperationFlags.Ok;
            }

            if (IsOthersHeldOrWaitConflictTabLock(req.Transaction, req.Index, req.Flags))
            {
                req.Flags |= LockFlags.Waiting;
                return CreateTabLockForWait(ref req, out entry);
            }

            entry = CreateTabLock(ref req);
            return DBOperationFlags.Ok;
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
        private bool IsOthersHeldOrWaitConflictRecLock(Transaction tx, PagePosition page, int slot, LockFlags flags, LockEntry endEntry = null)
        {
            var locks = RecLocks.GetValueOrDefault(page);
            if (locks == null)
            {
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var entry = node.Value;
                if (entry == endEntry)
                {
                    break;
                }

                if (entry.Transaction == tx ||
                    entry.GetBit(slot) == 0)
                {
                    continue;
                }

                if (entry.IsExclusive || flags.IsExclusive())
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
        private bool IsOthersHeldOrWaitConflictTabLock(Transaction tx, Index index, LockFlags flags, LockEntry endEntry = null)
        {
            var locks = TabLocks.GetValueOrDefault(index);
            if (locks == null)
            {
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var entry = node.Value;
                if (entry == endEntry)
                {
                    break;
                }

                if (entry.Transaction == tx)
                {
                    continue;
                }

                if (entry.IsExclusive || flags.IsExclusive())
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
                RecLocks[req.Position] = list; ;
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
                TabLocks[req.Index] = list; ;
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
            if (!recEntry.IsWaiting)
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
                return DBOperationFlags.Dead;
            }

            entry.Transaction.WaitEvent.Reset();
            return DBOperationFlags.Wait;
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
            if (!tabEntry.IsWaiting)
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
                return DBOperationFlags.Dead;
            }

            entry.Transaction.WaitEvent.Reset();
            return DBOperationFlags.Wait;
        }

        /// <summary>
        /// check if the lock caused dead-lock
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        private bool IsCausedDeadLock(LockEntry entry)
        {
            if (entry.Transaction.Locks.Count == 1 || !entry.IsWaiting)
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

            var lockBit = entry.IsTable ? 0 : entry.GetFirstBitSlot();
            if (lockBit < 0)
            {
                return false;
            }

            while (true)
            {
                var prevEntry = FindPrevLockEntry(entry);
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

                if (IsLockConflict(checkEntry, prevEntry))
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
            else
            {
                return entry.IsExclusive || other.IsExclusive;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MoveRecLockBits(PagePosition pos, int index)
        {
            var entry = FindFirstRecLockEntry(pos);

            while (entry != null)
            {
                entry.MoveBits(index);
                entry = FindNextLockEntry(entry);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ExtendRecLockCapacity(PagePosition pos, int count, LockExtendDirection direction)
        {
            var entry = FindFirstRecLockEntry(pos);

            while (entry != null)
            {
                entry.ExtendCapacity(count, direction);
                entry = FindNextLockEntry(entry);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindFirstRecLockEntry(PagePosition pos)
        {
            return RecLocks.TryGetValue(pos, out var list) ? list.First.Value : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindFirstTabLockEntry(Index index)
        {
            return TabLocks.TryGetValue(index, out var list) ? list.First.Value : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindNextLockEntry(LockEntry entry)
        {
            return entry?.GNode?.Next?.Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindPrevLockEntry(LockEntry entry)
        {
            return entry?.GNode?.Previous?.Value;
        }
    }
}