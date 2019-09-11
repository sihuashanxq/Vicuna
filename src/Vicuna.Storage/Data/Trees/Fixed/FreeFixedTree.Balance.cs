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

            ref var header = ref current.FixedHeader;
            var isRoot = current.IsRoot;
            var ctx = new SplitContext()
            {
                Index = index,
                Leaf = current,
                Current = current,
                Sibling = CreatePage(lltx, current.Depth, header.FileId, pageNumber, header.NodeFlags & (~TreeNodeFlags.Root), header.DataElementSize),
                OldSibling = ModifyPage(lltx, header.FileId, header.NextPageNumber)
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

            ref var header = ref current.FixedHeader;
            var entry = leaf.Remove(leaf.FixedHeader.Count - 1);
            var isRoot = current.IsRoot;
            var ctx = new SplitContext()
            {
                Index = index,
                Leaf = leaf,
                Current = current,
                Sibling = CreatePage(lltx, current.Depth, header.FileId, entry.Key, header.NodeFlags & (~TreeNodeFlags.Root), header.DataElementSize),
                OldSibling = ModifyPage(lltx, header.FileId, header.NextPageNumber)
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
            var root = ctx.Current;
            var entry = ctx.Leaf.Remove(ctx.Leaf.FixedHeader.Count - 1);

            ctx.Parent = root;
            ctx.Current = CreatePage(
                 lltx,
                 root.Depth,
                 root.Position.FileId,
                 entry.Key,
                 root.FixedHeader.NodeFlags & (~TreeNodeFlags.Root),
                 root.FixedHeader.DataElementSize
             );

            root.CopyEntriesTo(ctx.Current, 0);
            root.FixedHeader.NodeFlags = TreeNodeFlags.Branch | TreeNodeFlags.Root;
            root.FixedHeader.Depth--;
            root.FixedHeader.DataElementSize = sizeof(long);

            lltx.WriteFixedBTreeLeafPageDeleteEntry(ctx.Leaf.Position, ctx.Leaf.FixedHeader.Count);
            lltx.WriteFixedBTreeCopyEntries(root.Position, ctx.Current.Position, 0);
            lltx.WriteFixedBTreeRootSplitted(root.Position);

            SplitPage(lltx, ref ctx);
        }

        private void SplitPage(LowLevelTransaction lltx, ref SplitContext ctx)
        {
            ctx.Sibling.FixedHeader.PrevPageNumber = ctx.Current.FixedHeader.PageNumber;
            ctx.Current.FixedHeader.NextPageNumber = ctx.Sibling.FixedHeader.PageNumber;
            ctx.Current.CopyEntriesTo(ctx.Sibling, ctx.Index);

            lltx.WriteByte8(ctx.Sibling.Position, FreeFixedTreePageHeader.Offset("PrevPageNumber"), ctx.Current.FixedHeader.PageNumber);
            lltx.WriteByte8(ctx.Current.Position, FreeFixedTreePageHeader.Offset("NextPageNumber"), ctx.Sibling.FixedHeader.PageNumber);
            lltx.WriteFixedBTreeCopyEntries(ctx.Current.Position, ctx.Sibling.Position, ctx.Index);

            if (ctx.OldSibling != null)
            {
                ctx.OldSibling.FixedHeader.PrevPageNumber = ctx.Sibling.FixedHeader.PageNumber;
            }

            var key = ctx.Current.IsLeaf ?
                ctx.Sibling.FirstKey :
                ctx.Current.GetNodeEntry(ctx.Current.FixedHeader.Count - 1).Key;

            if (ctx.Parent == null)
            {
                ctx.Parent = GetPageForUpdate(lltx, key, ctx.Sibling.Depth - 1);
            }

            AddBranchEntry(lltx, ctx.Parent, ctx.Leaf, ctx.Current.FixedHeader.PageNumber, ctx.Sibling.FixedHeader.PageNumber, key);
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