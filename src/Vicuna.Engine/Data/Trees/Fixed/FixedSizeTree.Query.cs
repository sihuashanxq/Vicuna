using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FixedSizeTree
    {
        public FixedSizeTreeCursor GetCursor(LowLevelTransaction lltx)
        {
            return new FixedSizeTreeCursor()
            {
                Tree = this,
                Index = 0,
                Current = GetPageForQuery(lltx, long.MinValue, Constants.BTreeLeafPageDepth)
            };
        }

        public bool TryGetEntry(LowLevelTransaction lltx, long key, out FixedSizeTreeNodeEntry nodeEntry)
        {
            var page = GetPageForQuery(lltx, key, Constants.BTreeLeafPageDepth);
            if (page == null)
            {
                nodeEntry = FixedSizeTreeNodeEntry.Empty;
                return false;
            }

            if (true)
            {
                page.Search(key);
            }

            if (page.LastMatch != 0 || page.LastMatchIndex < 0)
            {
                nodeEntry = FixedSizeTreeNodeEntry.Empty;
                return false;
            }

            nodeEntry = page.GetNodeEntry(page.LastMatchIndex);
            return true;
        }
    }
}
