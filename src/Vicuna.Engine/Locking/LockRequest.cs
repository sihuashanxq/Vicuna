using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Locking
{
    public struct LockRequest
    {
        public Index Index;

        public int RecordIndex;

        public int RecordCount;

        public LockFlags Flags;

        public PagePosition Page;

        public Transaction Transaction;
    }
}
