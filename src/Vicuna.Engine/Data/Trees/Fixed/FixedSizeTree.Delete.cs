using System;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FixedSizeTree
    {
        public bool RemoveEntry(LowLevelTransaction lltx, long key, out FixedSizeTreeNodeEntry entry)
        {
            var page = GetPageForUpdate(lltx, key, Constants.BTreeLeafPageDepth);
            if (page == null)
            {
                entry = FixedSizeTreeNodeEntry.Empty;
                return false;
            }

            if (true)
            {
                page.Search(key);
            }

            if (page.LastMatch != 0 || page.LastMatchIndex < 0)
            {
                entry = FixedSizeTreeNodeEntry.Empty;
                return false;
            }

            if (page.FixedHeader.Count == 1)
            {
                lltx.WriteMultiLogBegin();
            }

            if (true)
            {
                entry = page.RemoveEntry(lltx, page.LastMatchIndex);
            }

            if (page.FixedHeader.Count == 0)
            {
                RemovePageRecursion(lltx, page, entry.Key);
                lltx.WriteMultiLogEnd();
            }

            return true;
        }

        protected void RemovePageRecursion(LowLevelTransaction lltx, FixedSizeTreePage page, long removedKey)
        {
            var branch = GetPageForUpdate(lltx, removedKey, (byte)(page.Depth - 1));
            if (branch != null && branch.FixedHeader.Count == 2)
            {
                RemoveBranchRecursion(lltx, page, branch);
                return;
            }

            ref var header = ref page.FixedHeader;
            if (header.PrevPageNumber != -1)
            {
                var prev = ModifyPage(lltx, header.FileId, header.PrevPageNumber);
                ref var prevHeader = ref prev.FixedHeader;
                prevHeader.NextPageNumber = header.NextPageNumber;

                lltx.WriteByte8(prev.Position, FixedSizeTreeHeader.Offset("NextPageNumber"), header.NextPageNumber);
            }

            if (header.NextPageNumber != -1)
            {
                var next = ModifyPage(lltx, header.FileId, header.NextPageNumber);
                ref var nextHeader = ref next.FixedHeader;
                nextHeader.PrevPageNumber = header.PrevPageNumber;

                lltx.WriteByte8(next.Position, FixedSizeTreeHeader.Offset("PrevPageNumber"), header.PrevPageNumber);
            }

            if (page.IsRoot)
            {
                header.Depth = Constants.BTreeLeafPageDepth;
                header.NodeFlags = TreeNodeFlags.Root | TreeNodeFlags.Leaf;
                header.NextPageNumber = -1;
                header.PrevPageNumber = -1;

                lltx.WriteFixedBTreeRootInitialized(page.Position);
                return;
            }

            if (branch == null)
            {
                throw new NullReferenceException($"parent is null for key:{page.LastKey}");
            }

            if (true)
            {
                branch.Search(removedKey);
            }

            if (branch.LastMatch != 0 || page.LastMatchIndex < 0)
            {
                throw new IndexOutOfRangeException($"lastmatch={branch.LastMatch},lastmatchindex={branch.LastMatchIndex}");
            }

            branch.RemoveEntry(lltx, branch.LastMatchIndex);
            lltx.WriteFixedBTreePageFreed(page.Position);
            AddEntry(lltx, header.PageNumber, Span<byte>.Empty, false);
        }

        protected void RemoveBranchRecursion(LowLevelTransaction lltx, FixedSizeTreePage page, FixedSizeTreePage branch)
        {
            var lastEntry = branch.GetNodeEntry(1);
            var firstEntry = branch.GetNodeEntry(0);

            var last = default(FixedSizeTreePage);
            var first = default(FixedSizeTreePage);

            ref var fixedHeader = ref page.FixedHeader;
            if (fixedHeader.PageNumber == firstEntry.PageNumber)
            {
                last = ModifyPage(lltx, fixedHeader.FileId, lastEntry.PageNumber);
                first = page;
            }
            else if (fixedHeader.PageNumber == lastEntry.PageNumber)
            {
                last = page;
                first = ModifyPage(lltx, fixedHeader.FileId, firstEntry.PageNumber);
            }

            if (last.FixedHeader.Count != 0 || first.FixedHeader.Count != 0)
            {
                return;
            }

            ref var lastHeader = ref last.FixedHeader;
            ref var firstHeader = ref first.FixedHeader;

            if (firstHeader.PrevPageNumber > 0)
            {
                var prev = ModifyPage(lltx, firstHeader.FileId, firstHeader.PrevPageNumber);
                ref var prevHeader = ref prev.FixedHeader;
                prevHeader.NextPageNumber = lastHeader.NextPageNumber;

                lltx.WriteByte8(prev.Position, FixedSizeTreeHeader.Offset("NextPageNumber"), lastHeader.NextPageNumber);
            }

            if (lastHeader.NextPageNumber > 0)
            {
                var next = ModifyPage(lltx, lastHeader.FileId, lastHeader.PrevPageNumber);
                ref var nextHeader = ref next.FixedHeader;
                nextHeader.PrevPageNumber = firstHeader.PrevPageNumber;

                lltx.WriteByte8(next.Position, FixedSizeTreeHeader.Offset("PrevPageNumber"), firstHeader.PrevPageNumber);
            }

            branch.RemoveEntry(lltx, 0);
            branch.RemoveEntry(lltx, 1);

            lltx.WriteFixedBTreePageFreed(last.Position);
            lltx.WriteFixedBTreePageFreed(first.Position);

            RemovePageRecursion(lltx, branch, firstEntry.Key);

            AddEntry(lltx, lastHeader.PageNumber, Span<byte>.Empty, false);
            AddEntry(lltx, firstHeader.PageNumber, Span<byte>.Empty, false);
        }
    }
}
