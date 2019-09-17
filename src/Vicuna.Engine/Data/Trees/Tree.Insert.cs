using System;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        public DBOperationFlags AddClusterEntry(LowLevelTransaction tx, KVTuple kv)
        {
            var cursor = GetCursorForUpdate(tx, kv.Key, Constants.BTreeLeafPageDepth);
            if (cursor.LastMatch == 0)
            {
                return UpdateClusterEntry(tx, cursor, kv);
            }

            return AddClusterEntry(tx, cursor, kv);
        }

        protected DBOperationFlags AddClusterEntry(LowLevelTransaction tx, TreePage cursor, KVTuple kv)
        {
            if (kv.Length > MaxEntrySizeInPage)
            {
                return AddClusterOverflowEntry(tx, cursor, kv);
            }

            if (!cursor.Alloc(cursor.LastMatchIndex, (ushort)kv.Length, TreeNodeHeaderFlags.Primary, out var entry))
            {
                //SplitPage();
            }

            ref var h = ref entry.Header;
            ref var t = ref entry.Transaction;

            kv.Key.CopyTo(entry.Key);
            kv.Value.CopyTo(entry.Value);

            h.IsDeleted = false;
            h.KeySize = (ushort)kv.Key.Length;
            h.DataSize = (ushort)kv.Value.Length;
            h.NodeFlags = TreeNodeHeaderFlags.Primary;
            t.TransactionNumber = tx.Id;
            t.TransactionRollbackNumber = -1;

            return DBOperationFlags.Ok;
        }

        protected DBOperationFlags AddClusterOverflowEntry(LowLevelTransaction tx, TreePage cursor, KVTuple kv)
        {
            return DBOperationFlags.Ok;
        }
    }
}