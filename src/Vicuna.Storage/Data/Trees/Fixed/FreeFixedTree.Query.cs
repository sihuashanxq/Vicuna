using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FreeFixedTree
    {
        public FreeFixedTreeCursor GetCursor(LowLevelTransaction lltx)
        {
            return new FreeFixedTreeCursor()
            {
                Tree = this,
                Index = 0,
                Current = GetPageForQuery(lltx, long.MinValue, Constants.PageDepth)
            };
        }

        public bool TryGetEntry(LowLevelTransaction lltx, long key, out FreeFixedTreeNodeEntry nodeEntry)
        {
            var page = GetPageForQuery(lltx, key, Constants.PageDepth);
            if (page == null)
            {
                nodeEntry = FreeFixedTreeNodeEntry.Empty;
                return false;
            }

            if (true)
            {
                page.Search(key);
            }

            if (page.LastMatch != 0 || page.LastMatchIndex < 0)
            {
                nodeEntry = FreeFixedTreeNodeEntry.Empty;
                return false;
            }

            nodeEntry = page.GetNodeEntry(page.LastMatchIndex);
            return true;
        }
    }
}
