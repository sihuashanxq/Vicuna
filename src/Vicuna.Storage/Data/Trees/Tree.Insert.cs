using System;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        public DBOperationFlags AddClusterEntry(LowLevelTransaction tx, Span<byte> key, Span<byte> value)
        {
            var cursor = GetCursorForUpdate(tx, key, -1);
            if (cursor.LastMatch != 0 || cursor.IsBranch)
            {
                return AddClusterEntry(tx, cursor, key, value);
            }

            var flags = LockRec(tx, cursor, LockFlags.Exclusive | LockFlags.Document);
            if (flags == DBOperationFlags.Success)
            {
                return flags;
            }

            if (IsDuplicateKey(tx, cursor))
            {
                throw new InvalidOperationException($"duplicate key for {key.ToString()}");
            }

            return AddClusterEntry(tx, cursor, key, value);
        }

        /// <summary>
        /// add clusterd k-v
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

        /// <summary>
        /// </summary>
        /// <param name="tx"></param>
        /// <param name="cursor"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        protected DBOperationFlags LockRec(LowLevelTransaction tx, TreePageCursor cursor, LockFlags flags)
        {
            var req = new LockRequest()
            {
                Flags = flags,
                Index = Index,
                Position = cursor.Current.Position,
                Transaction = tx.Transaction,
                RecordSlot = cursor.LastMatchIndex,
                RecordCount = cursor.TreeHeader.Count,
            };

            return EngineEnviorment.LockManager.Lock(ref req);
        }

        /// <summary>
        /// check if the key is duplicated
        /// </summary>
        /// <param name="tx"></param>
        /// <param name="cursor"></param>
        /// <returns></returns>
        protected bool IsDuplicateKey(LowLevelTransaction tx, TreePageCursor cursor)
        {
            return Index.IsUnique && !cursor.GetNodeHeader(cursor.LastMatchIndex).IsDeleted;
        }
    }
}
