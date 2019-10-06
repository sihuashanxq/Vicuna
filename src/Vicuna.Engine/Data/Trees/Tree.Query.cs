using System;
using System.Collections.Generic;
using System.Text;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        private Stack<TreePage> GetPagesForSplit(LowLevelTransaction lltx, Span<byte> key, ushort entrySize)
        {
            var path = new Stack<TreePage>();
            var buffer = lltx.Buffers.GetEntry(_root.FileId, _root.PageNumber);

            while (true)
            {
                var page = lltx.EnterWrite(buffer).AsTree();
                ref var header = ref page.TreeHeader;
                var freeSize = header.FreeSize;
                var isLeaf = header.IsLeaf;
                if (isLeaf)
                {
                    if (freeSize > entrySize)
                    {
                        while (path.Count != 0)
                        {
                            lltx.ExitLatch(path.Pop().Position);
                        }
                    }

                    path.Push(page);
                    break;
                }
                else if (freeSize > MaxBranchEntrySize)
                {
                    while (path.Count != 0)
                    {
                        lltx.ExitLatch(path.Pop().Position);
                    }
                }

                path.Push(page);
                buffer = lltx.Buffers.GetEntry(page.FindPage(key));
            }

            return path;
        }

        private TreePage GetPageForKey(LowLevelTransaction lltx, Span<byte> key, byte depth, LatchFlags latchFlags, TreeNodeEnumMode mode = TreeNodeEnumMode.Lte)
        {
            var latch = default(LatchScope);
            var buffer = lltx.Buffers.GetEntry(_root.FileId, _root.PageNumber);

            while (true)
            {
                //read before latched,it's safe?
                var page = lltx.EnterLatch(buffer, IsBranch(buffer) ? LatchFlags.Read : latchFlags).AsTree(mode);
                if (page.Depth == depth)
                {
                    latch?.Dispose();
                    return page;
                }

                latch?.Dispose();
                latch = lltx.RemoveLatch(buffer);
                buffer = lltx.Buffers.GetEntry(page.FindPage(key));
            }
        }

        private TreePage GetPageForQuery(LowLevelTransaction lltx, Span<byte> key, byte depth, TreeNodeEnumMode mode = TreeNodeEnumMode.Lte)
        {
            return GetPageForKey(lltx, key, depth, LatchFlags.Read, mode);
        }

        private TreePage GetPageForUpdate(LowLevelTransaction lltx, Span<byte> key, byte depth)
        {
            return GetPageForKey(lltx, key, depth, LatchFlags.Write, TreeNodeEnumMode.Lte);
        }
    }
}
