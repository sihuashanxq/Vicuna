using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FreeFixedTree
    {
        private SplitContext SplitLeaf(LowLevelTransaction lltx, FreeFixedTreePage current, long pageNumber, int index)
        {
            if (!current.IsLeaf)
            {
                throw new InvalidOperationException($"page:{current.Position} is not a leaf page");
            }

            ref var currentHeader = ref current.FixedHeader;
            var isRoot = current.IsRoot;
            var ctx = new SplitContext()
            {
                Index = index,
                Leaf = current,
                Current = current,
                Sibling = CreatePage(lltx, current.Depth, currentHeader.FileId, pageNumber, currentHeader.NodeFlags & (~TreeNodeFlags.Root), currentHeader.DataElementSize),
                OldSibling = ModifyPage(lltx, currentHeader.FileId, currentHeader.NextPageNumber)
            };

            if (isRoot)
            {
                SplitRoot(lltx, ref ctx);
            }
            else
            {
                SplitPage(lltx, ref ctx);
            }

            return ctx;
        }

        private SplitContext SplitBranch(LowLevelTransaction lltx, FreeFixedTreePage current, FreeFixedTreePage leaf, int index)
        {
            if (!current.IsBranch)
            {
                throw new InvalidOperationException($"page:{current.Position} is not a branch page");
            }

            ref var currentHeader = ref current.FixedHeader;
            var entry = leaf.Remove(leaf.FixedHeader.Count - 1);
            var isRoot = current.IsRoot;
            var ctx = new SplitContext()
            {
                Index = index,
                Leaf = leaf,
                Current = current,
                Sibling = CreatePage(lltx, current.Depth, currentHeader.FileId, entry.Key, currentHeader.NodeFlags & (~TreeNodeFlags.Root), currentHeader.DataElementSize),
                OldSibling = ModifyPage(lltx, currentHeader.FileId, currentHeader.NextPageNumber)
            };

            if (isRoot)
            {
                SplitRoot(lltx, ref ctx);
            }
            else
            {
                SplitPage(lltx, ref ctx);
            }

            return ctx;
        }

        private void SplitRoot(LowLevelTransaction lltx, ref SplitContext ctx)
        {
            ref var rootHeader = ref ctx.Current.FixedHeader;
            var root = ctx.Current;
            var entry = ctx.Leaf.Remove(ctx.Leaf.FixedHeader.Count - 1);

            ctx.Parent = root;
            ctx.Current = CreatePage(
                 lltx,
                 root.Depth,
                 root.Position.FileId,
                 entry.Key,
                 rootHeader.NodeFlags & (~TreeNodeFlags.Root),
                 rootHeader.DataElementSize
             );

            root.CopyEntriesTo(ctx.Current, 0);

            rootHeader.Depth--;
            rootHeader.NodeFlags = TreeNodeFlags.Branch | TreeNodeFlags.Root;
            rootHeader.DataElementSize = sizeof(long);

            lltx.WriteFixedBTreePageDeleteEntry(ctx.Leaf.Position, ctx.Leaf.FixedHeader.Count);
            lltx.WriteFixedBTreeCopyEntries(root.Position, ctx.Current.Position, 0);
            lltx.WriteFixedBTreeRootSplitted(root.Position);

            SplitPage(lltx, ref ctx);
        }

        private void SplitPage(LowLevelTransaction lltx, ref SplitContext ctx)
        {
            var current = ctx.Current;
            var sibling = ctx.Sibling;
            var oldSibling = ctx.OldSibling;

            ref var siblingHeader = ref sibling.FixedHeader;
            ref var currentHeader = ref current.FixedHeader;

            siblingHeader.PrevPageNumber = currentHeader.PageNumber;
            currentHeader.NextPageNumber = siblingHeader.PageNumber;

            current.CopyEntriesTo(sibling, ctx.Index);

            lltx.WriteFixedBTreeCopyEntries(current.Position, sibling.Position, ctx.Index);
            lltx.WriteByte8(current.Position, FreeFixedTreePageHeader.Offset("NextPageNumber"), siblingHeader.PageNumber);
            lltx.WriteByte8(sibling.Position, FreeFixedTreePageHeader.Offset("PrevPageNumber"), currentHeader.PageNumber);

            if (oldSibling != null)
            {
                ref var oldSiblingHeader = ref ctx.OldSibling.FixedHeader;

                siblingHeader.NextPageNumber = oldSiblingHeader.PageNumber;
                oldSiblingHeader.PrevPageNumber = siblingHeader.PageNumber;

                lltx.WriteByte8(sibling.Position, FreeFixedTreePageHeader.Offset("NextPageNumber"), siblingHeader.NextPageNumber);
                lltx.WriteByte8(oldSibling.Position, FreeFixedTreePageHeader.Offset("PrevPageNumber"), oldSiblingHeader.PrevPageNumber);
            }

            var key = current.IsLeaf ? sibling.FirstKey : current.GetNodeEntry(currentHeader.Count - 1).Key;
            var parent = ctx.Parent;
            if (parent == null)
            {
                ctx.Parent = parent = GetPageForUpdate(lltx, key, (byte)(siblingHeader.Depth - 1));
            }

            AddBranchEntry(lltx, parent, ctx.Leaf, currentHeader.PageNumber, siblingHeader.PageNumber, key);
        }

        private FreeFixedTreePage ModifyPage(LowLevelTransaction lltx, int fileId, long pageNumber)
        {
            if (pageNumber < 0 || fileId < 0)
            {
                return null;
            }

            return lltx.ModifyPage(fileId, pageNumber).AsFixed();
        }

        private FreeFixedTreePage CreatePage(LowLevelTransaction lltx, byte depth, int fileId, long pageNumber, TreeNodeFlags flags, byte dataSize)
        {
            var fixedPage = ModifyPage(lltx, fileId, pageNumber);
            if (fixedPage == null)
            {
                throw new NullReferenceException(nameof(fixedPage));
            }

            ref var fixedHeader = ref fixedPage.FixedHeader;
            var lsn = fixedHeader.LSN;

            Unsafe.InitBlock(ref fixedPage.Data[0], 0, Constants.PageHeaderSize);

            fixedHeader.Count = 0;
            fixedHeader.FileId = fileId;
            fixedHeader.NodeFlags = flags;
            fixedHeader.DataElementSize = dataSize;
            fixedHeader.PageNumber = pageNumber;
            fixedHeader.PrevPageNumber = -1;
            fixedHeader.NextPageNumber = -1;
            fixedHeader.Flags = PageHeaderFlags.BTree;
            fixedHeader.LSN = lsn;
            fixedHeader.Depth = depth;

            lltx.WriteFixedBTreePageCreated(new PagePosition(fileId, pageNumber), flags, depth, dataSize);

            return fixedPage;
        }

        protected struct SplitContext
        {
            public int Index;

            public FreeFixedTreePage Leaf;

            public FreeFixedTreePage Parent;

            public FreeFixedTreePage Current;

            public FreeFixedTreePage Sibling;

            public FreeFixedTreePage OldSibling;
        }
    }
}