using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Locking
{
    public struct LockRequest
    {
        public Index Index;

        public int RecordSlot;

        public int RecordCount;

        public LockFlags Flags;

        public PagePosition Position;

        public Transaction Transaction;
    }
}
