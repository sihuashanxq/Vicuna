using System;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Locking
{
    public class LockManager
    {
        public LockFlags LockTable(LowLevelTransaction tx, object index, LockFlags flags)
        {
            return LockFlags.Document;
        }

        public LockFlags LockClustered(LowLevelTransaction tx, object index, PagePosition pos, int slot, LockFlags flags)
        {
            return LockFlags.Document;
        }

        public LockFlags LockUniversal(LowLevelTransaction tx, object index, Span<byte> clusterdKey, LockFlags flags)
        {
            return LockFlags.Document;
        }
    }
}
