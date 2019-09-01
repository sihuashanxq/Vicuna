using System;
using System.Collections.Generic;
using System.Diagnostics;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FreeFixedTree
    {
        public DBOperationFlags SplitLeafPage(LowLevelTransaction lltx, FreeFixedTreePage left, long pageNumber, int index)
        {
            Debug.Assert(left.IsLeaf);

            var ctx = new FreeFixedFreeSplitContext()
            {
                Left = left,
                Index = index
            };
            ref var fixedHeader = ref left.FixedHeader;

            ctx.Right = lltx.ModifyPage(fixedHeader.FileId, pageNumber).AsFixed(left.Level);
            ctx.Right.InitPage(lltx, fixedHeader.FileId, pageNumber, fixedHeader.NodeFlags, fixedHeader.DataElementSize);
            ctx.OldRight = fixedHeader.NextPageNumber == -1 ?
                null :
                lltx.ModifyPage(fixedHeader.FileId, fixedHeader.NextPageNumber).AsFixed(left.Level);

            return ctx.Left.IsRoot ? SplitRootLeaf(lltx, ref ctx) : SplitLeafPage(lltx, ref ctx);
        }

        public DBOperationFlags SplitRootLeaf(LowLevelTransaction lltx, ref FreeFixedFreeSplitContext ctx)
        {
            return DBOperationFlags.Ok;
        }

        public DBOperationFlags SplitLeafPage(LowLevelTransaction lltx, ref FreeFixedFreeSplitContext ctx)
        {
            Debug.Assert(!ctx.Left.IsRoot && ctx.Left.Level != 0);

            ctx.Left.CopyEntriesTo(ctx.Right, ctx.Index);
            ctx.Left.FixedHeader.NextPageNumber = ctx.Right.FixedHeader.PageNumber;
            ctx.Right.FixedHeader.PrevPageNumber = ctx.Left.FixedHeader.PageNumber;

            if (ctx.OldRight != null)
            {
                ctx.OldRight.FixedHeader.PrevPageNumber = ctx.Right.FixedHeader.PageNumber;
            }

            //TODO:Log
            var parent = GetPageForUpdate(lltx, ctx.Right.FirstKey, ctx.Right.Level - 1);
            if (parent == null)
            {
                throw new NullReferenceException(nameof(parent));
            }

            parent.Search(ctx.Right.FirstKey);
            return SplitLeafPage(lltx, ref ctx, parent);
        }

        public DBOperationFlags SplitLeafPage(LowLevelTransaction lltx, ref FreeFixedFreeSplitContext ctx, FreeFixedTreePage parent)
        {
            if (parent.FixedHeader.Count == 0)
            {
                parent.Alloc(0, out var e1);
                parent.Alloc(1, out var e2);

                e1.Key = ctx.Right.FirstKey;
                e1.PageNumber = ctx.Left.Header.PageNumber;

                e2.Key = long.MaxValue;
                e2.PageNumber = ctx.Right.Header.PageNumber;

                return DBOperationFlags.Ok;
            }

            if (!parent.Alloc(parent.LastMatchIndex, out var e3))
            {
                //SplitBranch();
            }

            ref var e4 = ref parent.GetNodeHeader(parent.LastMatchIndex + 1);

            e3.Key = ctx.Right.FirstKey;
            e3.PageNumber = ctx.Left.Header.PageNumber;
            e4.PageNumber = ctx.Right.Header.PageNumber;

            return DBOperationFlags.Ok;
        }
    }

    public ref struct FreeFixedFreeSplitContext
    {
        public int Index;

        public FreeFixedTreePage Left;

        public FreeFixedTreePage Right;

        public FreeFixedTreePage OldRight;

        public List<FreeFixedTreePage> Path;
    }
}
