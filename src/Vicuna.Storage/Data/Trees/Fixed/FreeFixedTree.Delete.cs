using System;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FreeFixedTree
    {
        public bool Remove(LowLevelTransaction lltx, long key, out FreeFixedTreeNodeEntry entry)
        {
            var page = GetPageForQuery(lltx, key, Constants.PageDepth);
            if (page == null)
            {
                entry = FreeFixedTreeNodeEntry.Empty;
                return false;
            }

            if (true)
            {
                page.Search(key);
            }

            if (page.LastMatch != 0 || page.LastMatchIndex < 0)
            {
                entry = FreeFixedTreeNodeEntry.Empty;
                return false;
            }

            if (true)
            {
                entry = page.Remove(page.LastMatchIndex);
            }

            if (page.FixedHeader.Count == 0)
            {
                RemovePageRecursion(lltx, page, entry.Key);
            }

            return true;
        }

        protected void RemovePageRecursion(LowLevelTransaction lltx, FreeFixedTreePage page, long lastKey)
        {
            var parent = GetPageForUpdate(lltx, lastKey, (byte)(page.Depth - 1));
            if (parent != null && parent.FixedHeader.Count == 2)
            {
                RemoveParentRecursion(lltx, page, parent);
                return;
            }

            ref var fixedHeader = ref page.FixedHeader;
            if (fixedHeader.PrevPageNumber != -1)
            {
                var prev = ModifyPage(lltx, fixedHeader.FileId, fixedHeader.PrevPageNumber);
                ref var prevHeader = ref prev.FixedHeader;
                prevHeader.NextPageNumber = fixedHeader.NextPageNumber;

                lltx.WriteByte8(prev.Position, FreeFixedTreePageHeader.Offset("NextPageNumber"), fixedHeader.NextPageNumber);
            }

            if (fixedHeader.NextPageNumber != -1)
            {
                var next = ModifyPage(lltx, fixedHeader.FileId, fixedHeader.NextPageNumber);
                ref var nextHeader = ref next.FixedHeader;
                nextHeader.PrevPageNumber = fixedHeader.PrevPageNumber;

                lltx.WriteByte8(next.Position, FreeFixedTreePageHeader.Offset("PrevPageNumber"), fixedHeader.PrevPageNumber);
            }

            if (page.IsRoot)
            {
                fixedHeader.Depth = Constants.PageDepth;
                fixedHeader.NodeFlags = TreeNodeFlags.Root | TreeNodeFlags.Leaf;
                fixedHeader.NextPageNumber = -1;
                fixedHeader.PrevPageNumber = -1;

                lltx.WriteFixedBTreeRootInitialized(page.Position);
                return;
            }

            if (parent == null)
            {
                throw new NullReferenceException($"parent is null for key:{page.LastKey}");
            }

            if (true)
            {
                parent.Search(lastKey);
            }

            if (parent.LastMatch != 0 || page.LastMatchIndex < 0)
            {
                throw new IndexOutOfRangeException($"lastmatch={parent.LastMatch},lastmatchindex={parent.LastMatchIndex}");
            }

            parent.Remove(page.LastMatchIndex);
            lltx.WriteFixedBTreePageDeleteEntry(parent.Position, (ushort)parent.LastMatchIndex);
            //AddEntry(lltx, fixedHeader.PageNumber, Span<byte>.Empty);
        }

        protected void RemoveParentRecursion(LowLevelTransaction lltx, FreeFixedTreePage page, FreeFixedTreePage parent)
        {
            var lastEntry = parent.GetNodeEntry(1);
            var firstEntry = parent.GetNodeEntry(0);

            var last = default(FreeFixedTreePage);
            var first = default(FreeFixedTreePage);

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

                lltx.WriteByte8(prev.Position, FreeFixedTreePageHeader.Offset("NextPageNumber"), lastHeader.NextPageNumber);
            }

            if (lastHeader.NextPageNumber > 0)
            {
                var next = ModifyPage(lltx, lastHeader.FileId, lastHeader.PrevPageNumber);
                ref var nextHeader = ref next.FixedHeader;
                nextHeader.PrevPageNumber = firstHeader.PrevPageNumber;

                lltx.WriteByte8(next.Position, FreeFixedTreePageHeader.Offset("PrevPageNumber"), firstHeader.PrevPageNumber);
            }

            parent.Remove(0);
            parent.Remove(1);

            RemovePageRecursion(lltx, parent, firstEntry.Key);
            //AddEntry(lltx, fixedHeader.PageNumber, Span<byte>.Empty);
            //AddEntry(lltx, fixedHeader.PageNumber, Span<byte>.Empty);
        }

    }
}
