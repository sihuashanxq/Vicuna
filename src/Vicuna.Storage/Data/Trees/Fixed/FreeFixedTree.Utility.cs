using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FreeFixedTree
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsLeaf(BufferEntry buffer)
        {
            return buffer.Page.Header.Cast<FreeFixedTreePageHeader>().NodeFlags.HasFlag(TreeNodeFlags.Leaf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsBranch(BufferEntry buffer)
        {
            return buffer.Page.Header.Cast<FreeFixedTreePageHeader>().NodeFlags.HasFlag(TreeNodeFlags.Branch);
        }

        private BufferEntry GetBufferForKey(LowLevelTransaction lltx, long key, int target, out int level)
        {
            var i = 0;
            var buffer = lltx.Buffers.GetEntry(Root.FileId, Root.PageNumber);

            using (var tx = lltx.StartNew())
            {
                while (true)
                {
                    if (IsLeaf(buffer) || target == i)
                    {
                        break;
                    }

                    var page = tx.GetPage(buffer).AsFixed(i);
                    var next = page.SearchPage(key);

                    buffer = tx.Buffers.GetEntry(next);
                    i++;
                }
            }

            level = i;
            return buffer;
        }

        private FreeFixedTreePage GetPageForUpdate(LowLevelTransaction lltx, long key, int target = -1)
        {
            var buffer = GetBufferForKey(lltx, key, target, out var level);
            if (buffer == null)
            {
                throw new NullReferenceException($"can't find a page for the key:{key.ToString()}");
            }

            var page = lltx.ModifyPage(buffer).AsFixed(level);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            page.Search(key);
            return page;
        }

        private FreeFixedTreePage GetCursorForQuery(LowLevelTransaction lltx, long key, int target = -1, TreeNodeFetchMode mode = TreeNodeFetchMode.Lte)
        {
            var buffer = GetBufferForKey(lltx, key, target, out var level);
            if (buffer == null)
            {
                throw new NullReferenceException($"can't find a page for the key:{key.ToString()}");
            }

            var page = lltx.GetPage(buffer).AsFixed(level);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            page.Search(key);
            return page;
        }
    }
}
