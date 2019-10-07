using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        protected DBResult UpdateClusterEntry(LowLevelTransaction tx, TreePage page, KVTuple kv)
        {
            var flags = LockRec(tx, page, page.LastMatchIndex, LockFlags.Exclusive | LockFlags.Document);
            if (flags != DBResult.Success)
            {
                return flags;
            }

            if (!page.TryGetNodeEntry(page.LastMatchIndex, out var entry))
            {
                throw new InvalidOperationException($"read the tree's entry for {kv.Key.ToString()} failed at page:{page.Position}! ");
            }

            if (!entry.Header.IsDeleted)
            {
                throw new InvalidOperationException($"duplicate key for {kv.Key.ToString()}");
            }

            if (entry.VersionHeader.TransactionNumber == tx.Id)
            {
                return UpdateClusterEntry(tx, page, kv, ref entry, entry.VersionHeader.TransactionRollbackNumber);
            }

            var undo = BackUpUndoEntry(tx, page, page.LastMatchIndex);
            if (undo == -1)
            {
                throw new InvalidOperationException("back up undo failed!");
            }

            return UpdateClusterEntry(tx, page, kv, ref entry, undo);
        }

        protected DBResult UpdateClusterEntry(LowLevelTransaction tx, TreePage cursor, KVTuple kv, ref TreeNodeEntry oldEntry, long rollbackNumber)
        {
            throw null;
        }
    }
}