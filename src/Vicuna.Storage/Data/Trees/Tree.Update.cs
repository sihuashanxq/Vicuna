using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        protected DBOperationFlags UpdateClusterEntry(LowLevelTransaction tx, TreePageCursor cursor, KVTuple kv)
        {
            var flags = LockRec(tx, cursor, LockFlags.Exclusive | LockFlags.Document);
            if (flags == DBOperationFlags.Ok)
            {
                return flags;
            }

            if (!cursor.TryGetNode(cursor.LastMatchIndex, out var entry))
            {
                throw new InvalidOperationException($"read the tree's entry for {kv.Key.ToString()} failed at page:{cursor.Current.Position}! ");
            }

            if (!entry.Header.IsDeleted)
            {
                throw new InvalidOperationException($"duplicate key for {kv.Key.ToString()}");
            }

            if (entry.Transaction.TransactionNumber == tx.Id)
            {
                return UpdateClusterEntry(tx, cursor, kv, ref entry, entry.Transaction.TransactionRollbackNumber);
            }

            var undo = BackUpUndoEntry(tx, cursor, cursor.LastMatchIndex);
            if (undo == -1)
            {
                throw new InvalidOperationException("back up undo failed!");
            }

            return UpdateClusterEntry(tx, cursor, kv, ref entry, undo);
        }

        protected DBOperationFlags UpdateClusterEntry(LowLevelTransaction tx, TreePageCursor cursor, KVTuple kv, ref TreeNodeEntry oldEntry, long rollbackNumber)
        {
            throw null;
        }
    }
}