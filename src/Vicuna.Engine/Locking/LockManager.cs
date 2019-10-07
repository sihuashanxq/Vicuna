using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Locking
{
    public class LockManager
    {
        public object SyncRoot { get; } = new object();

        public Dictionary<TableIndex, LinkedList<LockEntry>> TabLocks { get; }

        public Dictionary<PagePosition, LinkedList<LockEntry>> RecLocks { get; set; }

        public LockManager()
        {
            TabLocks = new Dictionary<TableIndex, LinkedList<LockEntry>>();
            RecLocks = new Dictionary<PagePosition, LinkedList<LockEntry>>();
        }

        /// <summary>
        /// lock a table
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        public DBResult Lock(ref LockContext ctx)
        {
            lock (SyncRoot)
            {
                return ctx.Flags.IsTable() ? LockTab(ref ctx, out var _) : LockRec(ref ctx, out var _);
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        public DBResult UnLock(LockEntry entry)
        {
            lock (SyncRoot)
            {
                if (entry == null)
                {
                    return DBResult.Success;
                }

                var list = entry.GNode.List;
                var next = FindNextLockEntry(entry);

                list.Remove(entry.GNode);

                return entry.IsTable ? UnLockTab(entry, next) : UnLockRec(entry, next);
            }
        }

        /// <summary>
        /// un-lock-rec and try wake up from the next to the end
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="next"></param>
        /// <returns></returns>
        internal DBResult UnLockRec(LockEntry entry, LockEntry next)
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

            return DBResult.Success;
        }

        /// <summary>
        /// un-lock-tab and try wake up from the next to the end
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="next"></param>
        /// <returns></returns>
        internal DBResult UnLockTab(LockEntry entry, LockEntry next)
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

            return DBResult.Success;
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

            if (list.Count != 0)
            {
                RecLocks[to] = list;
            }
        }

        private unsafe LockEntry SplitRecLock(LockEntry fromEntry, PagePosition to, int mid)
        {
            if (fromEntry == null)
            {
                return null;
            }

            var count = fromEntry.Bits.Length - (mid >> 3) + 8;
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
        /// <param name="ctx"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        public DBResult LockRec(ref LockContext ctx, out LockEntry entry)
        {
            var locks = RecLocks.GetValueOrDefault(ctx.Page);
            if (locks == null || locks.Count == 0)
            {
                entry = CreateRecLock(ref ctx);
                return DBResult.Success;
            }

            if (IsHeldTabLock(ref ctx, out entry))
            {
                return DBResult.Success;
            }

            if (IsHeldRecLock(ref ctx, out entry))
            {
                return DBResult.Success;
            }

            if (IsOthersHeldOrWaitConflictRecLock(ctx.Transaction, ctx.Page, ctx.RecordIndex, ctx.Flags))
            {
                ctx.Flags |= LockFlags.Waiting;
                return CreateRecLockForWait(ref ctx, out entry);
            }

            entry = GetCanReuseRecLock(ref ctx) ?? CreateRecLock(ref ctx);
            return DBResult.Success;
        }

        /// <summary>
        /// lock a table
        /// </summary>
        /// <param name="ctx"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        public DBResult LockTab(ref LockContext ctx, out LockEntry entry)
        {
            var locks = TabLocks.GetValueOrDefault(ctx.Index);
            if (locks == null || locks.Count == 0)
            {
                entry = CreateTabLock(ref ctx);
                return DBResult.Success;
            }

            if (IsHeldTabLock(ref ctx, out entry))
            {
                return DBResult.Success;
            }

            if (IsOthersHeldOrWaitConflictTabLock(ctx.Transaction, ctx.Index, ctx.Flags))
            {
                ctx.Flags |= LockFlags.Waiting;
                return CreateTabLockForWait(ref ctx, out entry);
            }

            entry = CreateTabLock(ref ctx);
            return DBResult.Success;
        }

        /// <summary>
        /// check current transaction has held the record's lock >=ctx-lock-level and not been in wait state
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        private bool IsHeldRecLock(ref LockContext ctx, out LockEntry entry)
        {
            var locks = RecLocks.GetValueOrDefault(ctx.Page);
            if (locks == null)
            {
                entry = null;
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.Transaction != ctx.Transaction ||
                    lockEntry.GetBit(ctx.RecordIndex) == 0)
                {
                    continue;
                }

                if (lockEntry.IsExclusive || !ctx.Flags.IsExclusive())
                {
                    entry = lockEntry;
                    return true;
                }
            }

            entry = null;
            return false;
        }

        /// <summary>
        /// check current transaction has held the table's lock >=ctx-lock-level and not been in wait state
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        private bool IsHeldTabLock(ref LockContext ctx, out LockEntry entry)
        {
            var locks = TabLocks.GetValueOrDefault(ctx.Index);
            if (locks == null)
            {
                entry = null;
                return false;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.Transaction != ctx.Transaction)
                {
                    continue;
                }

                if (lockEntry.IsExclusive || !ctx.Flags.IsExclusive())
                {
                    entry = lockEntry;
                    return true;
                }
            }

            entry = null;
            return false;
        }

        /// <summary>
        /// is other's transaction has held the recrod's lock and conflict with the ctx-lock
        /// </summary>
        /// <param name="ctx"></param>
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
        /// is other's transaction has held the table's lock and conflict with the ctx-lock
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        private bool IsOthersHeldOrWaitConflictTabLock(Transaction tx, TableIndex index, LockFlags flags, LockEntry endEntry = null)
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
        /// <param name="ctx"></param>
        /// <returns></returns>
        private LockEntry CreateRecLock(ref LockContext ctx)
        {
            if (!RecLocks.TryGetValue(ctx.Page, out var list))
            {
                list = new LinkedList<LockEntry>();
                RecLocks[ctx.Page] = list; ;
            }

            var tx = ctx.Transaction;
            var entry = new LockEntry(ctx.Page, ctx.Flags, ctx.RecordCount);
            if (entry.IsWaiting)
            {
                tx.WaitLock = entry;
            }

            list.AddLast(entry);
            tx.Locks.AddLast(entry);

            entry.GNode = list.Last;
            entry.TNode = tx.Locks.Last;
            entry.Index = ctx.Index;
            entry.Transaction = ctx.Transaction;
            entry.Thread = Thread.CurrentThread.ManagedThreadId;
            entry.SetBit(ctx.RecordIndex, 1);

            return entry;
        }

        /// <summary>
        /// create a new table-lock
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        private LockEntry CreateTabLock(ref LockContext ctx)
        {
            if (!TabLocks.TryGetValue(ctx.Index, out var list))
            {
                list = new LinkedList<LockEntry>();
                TabLocks[ctx.Index] = list; ;
            }

            var tx = ctx.Transaction;
            var entry = new LockEntry(ctx.Page, ctx.Flags, ctx.RecordCount);
            if (entry.IsWaiting)
            {
                tx.WaitLock = entry;
            }

            list.AddLast(entry);
            tx.Locks.AddLast(entry);

            entry.GNode = list.Last;
            entry.TNode = tx.Locks.Last;
            entry.Index = ctx.Index;
            entry.Transaction = ctx.Transaction;
            entry.Thread = Thread.CurrentThread.ManagedThreadId;

            return entry;
        }

        /// <summary>
        /// reuse the current transaction's created rec-lock
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        private LockEntry GetCanReuseRecLock(ref LockContext ctx)
        {
            var entry = default(LockEntry);
            var locks = RecLocks.GetValueOrDefault(ctx.Page);
            if (locks == null)
            {
                return null;
            }

            for (var node = locks.First; node != null; node = node.Next)
            {
                var lockEntry = node.Value;
                if (lockEntry.Transaction == ctx.Transaction &&
                    lockEntry.IsExclusive == ctx.Flags.IsExclusive())
                {
                    entry = lockEntry;
                    break;
                }
            }

            if (entry != null)
            {
                entry.SetBit(ctx.RecordIndex, 1);
            }

            return entry;
        }

        /// <summary>
        /// create new rec lock then waitting
        /// </summary>
        /// <param name="ctx"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        private DBResult CreateRecLockForWait(ref LockContext ctx, out LockEntry entry)
        {
            var recEntry = CreateRecLock(ref ctx);
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
                entry.SetBit(ctx.RecordIndex, 0);
                entry.Transaction.WaitLock = null;
                return DBResult.DeadLock;
            }

            entry.Transaction.WaitEvent.Reset();
            return DBResult.WaitLock;
        }

        /// <summary>
        /// create new rec lock then waitting
        /// </summary>
        /// <param name="ctx"></param>
        /// <param name="entry"></param>
        /// <returns></returns>
        private DBResult CreateTabLockForWait(ref LockContext ctx, out LockEntry entry)
        {
            var tabEntry = CreateTabLock(ref ctx);
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
                return DBResult.DeadLock;
            }

            entry.Transaction.WaitEvent.Reset();
            return DBResult.WaitLock;
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
        public void ExtendRecLockCap(PagePosition pos, int count, LockExtendDirection direction)
        {
            lock (SyncRoot)
            {
                var entry = FindFirstRecLockEntry(pos);

                while (entry != null)
                {
                    entry.ExtendCapacity(count, direction);
                    entry = FindNextLockEntry(entry);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExtendRecLockCap(PagePosition pos, int index, int count)
        {
            lock (SyncRoot)
            {
                var entry = FindFirstRecLockEntry(pos);

                while (entry != null)
                {
                    if (entry.Count < count + 1)
                    {
                        entry.ExtendCapacity(count - entry.Count + 64, LockExtendDirection.Tail);
                    }

                    if (index < count)
                    {
                        entry.MoveBits(index);
                    }

                    entry = FindNextLockEntry(entry);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindFirstRecLockEntry(PagePosition pos)
        {
            return RecLocks.TryGetValue(pos, out var list) ? list.First.Value : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LockEntry FindFirstTabLockEntry(TableIndex index)
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