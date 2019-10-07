using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        protected SplitContext SplitLeafPage(LowLevelTransaction lltx, TreePage current, Stack<TreePage> path, Span<byte> key, int index)
        {
            if (!current.IsLeaf)
            {
                throw new InvalidOperationException($"page:{current.Position} is not a leaf page");
            }

            ref var currentHeader = ref current.TreeHeader;
            var isRoot = current.IsRoot;
            var ctx = new SplitContext()
            {
                Index = index,
                Current = current,
                Sibling = AllocatePage(lltx, current.Depth, currentHeader.FileId, currentHeader.NodeFlags & (~TreeNodeFlags.Root)),
                OldSibling = ModifyPage(lltx, currentHeader.FileId, currentHeader.NextPageNumber)
            };

            if (isRoot)
            {
                SplitRootPage(lltx, ref ctx);
            }
            else
            {
                SplitPage(lltx, ref ctx, path);
            }

            return ctx;
        }

        private SplitContext SplitBranchPage(LowLevelTransaction lltx, TreePage current, Stack<TreePage> path, int index)
        {
            if (!current.IsBranch)
            {
                throw new InvalidOperationException($"page:{current.Position} is not a branch page");
            }

            ref var currentHeader = ref current.TreeHeader;
            var isRoot = current.IsRoot;
            var ctx = new SplitContext()
            {
                Index = index,
                Current = current,
                Sibling = AllocatePage(lltx, current.Depth, currentHeader.FileId, currentHeader.NodeFlags & (~TreeNodeFlags.Root)),
                OldSibling = ModifyPage(lltx, currentHeader.FileId, currentHeader.NextPageNumber)
            };

            if (isRoot)
            {
                SplitRootPage(lltx, ref ctx);
            }
            else
            {
                SplitPage(lltx, ref ctx, path);
            }

            return ctx;
        }

        private void SplitRootPage(LowLevelTransaction lltx, ref SplitContext ctx)
        {
            ref var rootHeader = ref ctx.Current.TreeHeader;
            var root = ctx.Current;

            ctx.Parent = root;
            ctx.Current = AllocatePage(lltx, root.Depth, root.Position.FileId, rootHeader.NodeFlags & (~TreeNodeFlags.Root));

            root.CopyEntriesTo(lltx, ctx.Current);

            rootHeader.Depth--;
            rootHeader.NodeFlags = TreeNodeFlags.Branch | TreeNodeFlags.Root;

            lltx.WriteFixedBTreeRootSplitted(root.Position);

            SplitPage(lltx, ref ctx, null);
        }

        private void SplitPage(LowLevelTransaction lltx, ref SplitContext ctx, Stack<TreePage> path)
        {
            var current = ctx.Current;
            var sibling = ctx.Sibling;
            var oldSibling = ctx.OldSibling;

            ref var siblingHeader = ref sibling.TreeHeader;
            ref var currentHeader = ref current.TreeHeader;
            var key = Span<byte>.Empty;

            ctx.Index = current.CopyEntriesTo(lltx, ctx.Index, sibling);

            if (ctx.Current.IsLeaf)
            {
                siblingHeader.PrevPageNumber = currentHeader.PageNumber;
                currentHeader.NextPageNumber = siblingHeader.PageNumber;

                lltx.WriteByte8(current.Position, TreeHelper.ByteOffset(ref currentHeader, ref currentHeader.NextPageNumber), currentHeader.NextPageNumber);
                lltx.WriteByte8(sibling.Position, TreeHelper.ByteOffset(ref siblingHeader, ref siblingHeader.PrevPageNumber), siblingHeader.PrevPageNumber);

                if (oldSibling != null)
                {
                    ref var oldSiblingHeader = ref ctx.OldSibling.TreeHeader;

                    siblingHeader.NextPageNumber = oldSiblingHeader.PageNumber;
                    oldSiblingHeader.PrevPageNumber = siblingHeader.PageNumber;

                    lltx.WriteByte8(sibling.Position, TreeHelper.ByteOffset(ref siblingHeader, ref siblingHeader.NextPageNumber), siblingHeader.NextPageNumber);
                    lltx.WriteByte8(oldSibling.Position, TreeHelper.ByteOffset(ref oldSiblingHeader, ref oldSiblingHeader.NextPageNumber), oldSiblingHeader.PrevPageNumber);
                }

                key = sibling.FirstKey;
            }
            else
            {
                key = current.GetNodeKey(currentHeader.Count - 1);
            }

            if (ctx.Parent == null)
            {
                ctx.Parent = path.Pop();
            }

            AddBranchPointerEntry(lltx, ctx.Parent, currentHeader.PageNumber, siblingHeader.PageNumber, key, path);
        }

        private TreePage ModifyPage(LowLevelTransaction lltx, int fileId, long pageNumber)
        {
            if (pageNumber < 0 || fileId < 0)
            {
                return null;
            }

            return lltx.EnterWrite(fileId, pageNumber).AsTree();
        }

        private TreePage AllocatePage(LowLevelTransaction lltx, byte depth, int fileId, TreeNodeFlags flags)
        {
            var page = lltx.AllocatePage(fileId);
            return CreatePage(lltx, depth, page.FileId, page.PageNumber, flags);
        }

        private TreePage CreatePage(LowLevelTransaction lltx, byte depth, int fileId, long pageNumber, TreeNodeFlags flags)
        {
            var page = ModifyPage(lltx, fileId, pageNumber);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            ref var treeHeader = ref page.TreeHeader;
            var lsn = treeHeader.LSN;

            Unsafe.InitBlock(ref page.Data[0], 0, Constants.PageHeaderSize);

            treeHeader.UsedSize = Constants.PageHeaderSize + Constants.PageFooterSize;
            treeHeader.Low = Constants.PageHeaderSize;
            treeHeader.Upper = Constants.PageSize - Constants.PageFooterSize;
            treeHeader.Count = 0;
            treeHeader.FileId = fileId;
            treeHeader.NodeFlags = flags;
            treeHeader.PageNumber = pageNumber;
            treeHeader.PrevPageNumber = -1;
            treeHeader.NextPageNumber = -1;
            treeHeader.Flags = PageHeaderFlags.BTree;
            treeHeader.LSN = lsn;
            treeHeader.Depth = depth;

            lltx.WriteBTreePageCreated(new PagePosition(fileId, pageNumber), flags, depth);

            return page;
        }

        protected struct SplitContext
        {
            public int Index;

            public TreePage Parent;

            public TreePage Current;

            public TreePage Sibling;

            public TreePage OldSibling;
        }
    }
}
