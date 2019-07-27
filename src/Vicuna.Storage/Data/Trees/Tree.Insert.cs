using System;
using System.Diagnostics;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public enum DBOperationFlags
    {
        Error,

        Success,

        Waitting
    }

    public partial class Tree
    {
        public DBOperationFlags AddClusterEntry(LowLevelTransaction tx, Span<byte> key, Span<byte> value)
        {
            var cursor = GetCursorForQuery(tx, key, -1, true);
            if (cursor.LastMatch != 0 || !cursor.IsLeaf || !Index.IsUnique)
            {
                return AddClusterEntry(tx, cursor, key, value);
            }

            var lockFlags = LockNodeEntry(tx, cursor, LockFlags.Exclusive | LockFlags.Document);
            if (lockFlags.IsWaitting())
            {
                return DBOperationFlags.Waitting;
            }

            if (IsUniqueDuplicateKey(tx, cursor))
            {
                throw new InvalidOperationException($"duplicate key for {key.ToString()}");
            }

            return DBOperationFlags.Success;
        }

        /// <summary>
        /// AddCluster None Locking
        /// </summary>
        /// <param name="tx"></param>
        /// <param name="cursor"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        protected DBOperationFlags AddClusterEntry(LowLevelTransaction tx, TreePageCursor cursor, Span<byte> key, Span<byte> value)
        {
            return DBOperationFlags.Success;
        }

        protected LockFlags LockNodeEntry(LowLevelTransaction tx, TreePageCursor cursor, LockFlags flags)
        {
            if (Index.IsClustered)
            {
                return tx.LockManager.LockClustered(tx, Index, cursor.Current.Position, cursor.LastMatchIndex, flags);
            }

            var clusterdKey = cursor.GetNodeData(cursor.LastMatchIndex);
#if (DEBUG)
            Debug.Assert(clusterdKey.Length == 0);
#endif
            return tx.LockManager.LockUniversal(tx, Index, clusterdKey, flags);
        }

        protected bool IsUniqueDuplicateKey(LowLevelTransaction tx, TreePageCursor cursor)
        {
            if (Index.IsUnique)
            {
                return false;
            }

            return !cursor.GetNodeHeaderWithIndex(cursor.LastMatchIndex).IsDeleted;
        }
    }
}
