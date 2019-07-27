using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsLeaf(BufferEntry buffer)
        {
            return buffer.Page.Header.Cast<TreePageHeader>().NodeFlags.HasFlag(TreeNodeFlags.Leaf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsBranch(BufferEntry buffer)
        {
            return buffer.Page.Header.Cast<TreePageHeader>().NodeFlags.HasFlag(TreeNodeFlags.Branch);
        }

        private BufferEntry GetBufferForKey(LowLevelTransaction ttx, Span<byte> key, int target, out int level)
        {
            var count = 0;
            var buffer = ttx.Buffers.GetBuffer(Root);

            using (var tx = ttx.StartNew())
            {
                while (true)
                {
                    if (IsLeaf(buffer) || target == count)
                    {
                        break;
                    }

                    var page = tx.GetPage(buffer);
                    var match = default(PagePosition);
                    var cursor = new TreePageCursor(page, count, TreeNodeFetchMode.Lte);

                    cursor.Search(key);
                    match = cursor.GetLastMatchedPage();
                    buffer = tx.Buffers.GetBuffer(match);

                    count++;
                }
            }

            level = count;
            return buffer;
        }

        private TreePageCursor GetCursorForUpdate(LowLevelTransaction ttx, Span<byte> key, int target = -1)
        {
            var buffer = GetBufferForKey(ttx, key, target, out var level);
            if (buffer == null)
            {
                throw new NullReferenceException($"can't find a page for the key:{key.ToString()}");
            }

            var page = ttx.ModifyPage(buffer);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            return new TreePageCursor(page, level, TreeNodeFetchMode.Lte).Search(key);
        }

        private TreePageCursor GetCursorForQuery(LowLevelTransaction ttx, Span<byte> key, int target = -1, TreeNodeFetchMode mode = TreeNodeFetchMode.Lte)
        {
            var buffer = GetBufferForKey(ttx, key, target, out var level);
            if (buffer == null)
            {
                throw new NullReferenceException($"can't find a page for the key:{key.ToString()}");
            }

            var page = ttx.GetPage(buffer);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            return new TreePageCursor(page, level, mode).Search(key);
        }
    }
}
