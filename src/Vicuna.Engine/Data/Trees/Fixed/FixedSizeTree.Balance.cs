using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FixedSizeTree
    {
        private SplitContext SplitLeaf(LowLevelTransaction lltx, FixedSizeTreePage current, long pageNumber, int index)
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

        private SplitContext SplitBranch(LowLevelTransaction lltx, FixedSizeTreePage current, FixedSizeTreePage leaf, int index)
        {
            if (!current.IsBranch)
            {
                throw new InvalidOperationException($"page:{current.Position} is not a branch page");
            }

            ref var currentHeader = ref current.FixedHeader;
            var entry = leaf.RemoveEntry(lltx, leaf.FixedHeader.Count - 1);
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
            var entry = ctx.Leaf.RemoveEntry(lltx, ctx.Leaf.FixedHeader.Count - 1);

            ctx.Parent = root;
            ctx.Current = CreatePage(
                 lltx,
                 root.Depth,
                 root.Position.FileId,
                 entry.Key,
                 rootHeader.NodeFlags & (~TreeNodeFlags.Root),
                 rootHeader.DataElementSize
             );

            root.CopyEntriesTo(lltx, 0, ctx.Current);

            rootHeader.Depth--;
            rootHeader.NodeFlags = TreeNodeFlags.Branch | TreeNodeFlags.Root;
            rootHeader.DataElementSize = sizeof(long);

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

            current.CopyEntriesTo(lltx, ctx.Index, sibling);

            if (ctx.Current.IsLeaf)
            {
                siblingHeader.PrevPageNumber = currentHeader.PageNumber;
                currentHeader.NextPageNumber = siblingHeader.PageNumber;

                lltx.WriteByte8(current.Position, FixedSizeTreeHeader.Offset("NextPageNumber"), siblingHeader.PageNumber);
                lltx.WriteByte8(sibling.Position, FixedSizeTreeHeader.Offset("PrevPageNumber"), currentHeader.PageNumber);

                if (oldSibling != null)
                {
                    ref var oldSiblingHeader = ref ctx.OldSibling.FixedHeader;

                    siblingHeader.NextPageNumber = oldSiblingHeader.PageNumber;
                    oldSiblingHeader.PrevPageNumber = siblingHeader.PageNumber;

                    lltx.WriteByte8(sibling.Position, FixedSizeTreeHeader.Offset("NextPageNumber"), siblingHeader.NextPageNumber);
                    lltx.WriteByte8(oldSibling.Position, FixedSizeTreeHeader.Offset("PrevPageNumber"), oldSiblingHeader.PrevPageNumber);
                }
            }

            var key = current.IsLeaf ? sibling.FirstKey : current.GetNodeEntry(currentHeader.Count - 1).Key;
            if (ctx.Parent == null)
            {
                ctx.Parent = GetPageForUpdate(lltx, key, (byte)(siblingHeader.Depth - 1));
            }

            AddBranchEntry(lltx, ctx.Parent, ctx.Leaf, currentHeader.PageNumber, siblingHeader.PageNumber, key);
        }

        private FixedSizeTreePage ModifyPage(LowLevelTransaction lltx, int fileId, long pageNumber)
        {
            if (pageNumber < 0 || fileId < 0)
            {
                return null;
            }

            return lltx.EnterLatch(fileId, pageNumber, LatchFlags.Write).AsFixed();
        }

        private FixedSizeTreePage CreatePage(LowLevelTransaction lltx, byte depth, int fileId, long pageNumber, TreeNodeFlags flags, byte dataSize)
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

            public FixedSizeTreePage Leaf;

            public FixedSizeTreePage Parent;

            public FixedSizeTreePage Current;

            public FixedSizeTreePage Sibling;

            public FixedSizeTreePage OldSibling;
        }
    }
}