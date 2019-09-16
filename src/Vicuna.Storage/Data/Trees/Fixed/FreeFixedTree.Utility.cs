using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Locking;
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

        private BufferEntry GetBufferForKey(LowLevelTransaction lltx, long key, byte depth)
        {
            var buffer = lltx.Buffers.GetEntry(Root.FileId, Root.PageNumber);

            using (var tx = lltx.StartNew())
            {
                while (true)
                {
                    if (IsLeaf(buffer))
                    {
                        break;
                    }

                    var page = lltx.HasBufferLatch(buffer, LatchFlags.Read) ? buffer.Page.AsFixed() : tx.GetPage(buffer).AsFixed();
                    if (page.Depth == depth)
                    {
                        break;
                    }

                    buffer = tx.Buffers.GetEntry(page.FindPage(key));
                }
            }

            return buffer;
        }

        private FreeFixedTreePage GetPageForUpdate(LowLevelTransaction lltx, long key, byte depth)
        {
            var buffer = GetBufferForKey(lltx, key, depth);
            if (buffer == null)
            {
                throw new NullReferenceException($"can't find a page for the key:{key.ToString()}");
            }

            var fixedPage = lltx.ModifyPage(buffer).AsFixed();
            if (fixedPage == null)
            {
                throw new NullReferenceException(nameof(fixedPage));
            }

            return fixedPage;
        }

        private FreeFixedTreePage GetPageForQuery(LowLevelTransaction lltx, long key, byte depth)
        {
            var buffer = GetBufferForKey(lltx, key, depth);
            if (buffer == null)
            {
                throw new NullReferenceException($"can't find a page for the key:{key.ToString()}");
            }

            var fixedPage = lltx.GetPage(buffer).AsFixed();
            if (fixedPage == null)
            {
                throw new NullReferenceException(nameof(fixedPage));
            }

            return fixedPage;
        }
    }
}
