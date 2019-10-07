using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Locking
{
    public struct LockContext
    {
        public TableIndex Index;

        public int RecordIndex;

        public int RecordCount;

        public LockFlags Flags;

        public PagePosition Page;

        public Transaction Transaction;
    }
}
