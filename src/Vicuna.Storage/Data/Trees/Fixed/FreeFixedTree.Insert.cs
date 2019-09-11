using System;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FreeFixedTree
    {
        protected System.Threading.ReaderWriterLockSlim _read = new System.Threading.ReaderWriterLockSlim();

        public void AddEntry(LowLevelTransaction lltx, long key, Span<byte> value)
        {
            lock (this)
            {
                var page = GetPageForUpdate(lltx, key, Constants.PageDepth);
                if (page == null)
                {
                    throw new InvalidOperationException($"can't find a page for add key:{key}");
                }

                AddEntry(lltx, page, key, value);
            }
        }

        protected void AddEntry(LowLevelTransaction lltx, FreeFixedTreePage page, long key, Span<byte> value)
        {
            if (!page.AllocForKey(key, out var matchFlags, out _, out var entry))
            {
                lltx.WriteMultiLogBegin();
                SplitLeaf(lltx, page, key, page.FixedHeader.Count / 2);
                lltx.WriteMultiLogEnd();
                return;
            }

            if (matchFlags == 0)
            {
                return;
            }

            if (page.FixedHeader.DataElementSize < value.Length)
            {
                throw new InvalidOperationException($@"the value-size:{value.Length} of node 
                        must be lessThan or equals:{page.FixedHeader.DataElementSize} bytes 
                        in page:{page.Position}");
            }

            entry.Key = key;
            value.CopyTo(entry.Value);

            lltx.WriteFixedBTreeLeafPageInsertEntry(page.Position, key, value);
        }

        protected void AddBranchEntry(LowLevelTransaction lltx, FreeFixedTreePage page, FreeFixedTreePage leaf, long lPageNumber, long rPageNumber, long key)
        {
            ref var fixedHeader = ref page.FixedHeader;
            if (!fixedHeader.NodeFlags.HasFlag(TreeNodeFlags.Branch))
            {
                throw new InvalidOperationException($"page:{page.Position} is not a branch page!");
            }

            if (fixedHeader.Count == 0)
            {
                page.Alloc(0, out var entry1);
                page.Alloc(1, out var entry2);

                entry1.Key = key;
                entry1.PageNumber = lPageNumber;

                entry2.Key = 0;
                entry2.PageNumber = rPageNumber;

                lltx.WriteFixedBTreeBranchPageInsertEntry(page.Position, key, lPageNumber, rPageNumber);
                return;
            }

            if (page.AllocForKey(key, out _, out var index, out var entry))
            {
                if (index == fixedHeader.Count - 1)
                {
                    var prevEntry = page.GetNodeEntry(index - 1);

                    prevEntry.Key = key;
                    entry.PageNumber = rPageNumber;
                }
                else
                {
                    var nextEntry = page.GetNodeEntry(index + 1);

                    entry.Key = key;
                    entry.PageNumber = lPageNumber;
                    nextEntry.PageNumber = rPageNumber;
                }

                lltx.WriteFixedBTreeBranchPageInsertEntry(page.Position, key, lPageNumber, rPageNumber);
            }
            else
            {
                var mid = fixedHeader.Count / 2;
                var ctx = SplitBranch(lltx, page, leaf, mid);
                if (mid <= index)
                {
                    AddBranchEntry(lltx, ctx.Sibling, leaf, lPageNumber, rPageNumber, key);
                }
                else
                {
                    AddBranchEntry(lltx, ctx.Current, leaf, lPageNumber, rPageNumber, key);
                }
            }
        }
    }
}