using System;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;
using Vicuna.Engine.Buffers;
using System.Collections;
using Vicuna.Engine.Locking;
using System.Collections.Generic;

namespace Vicuna.Engine.Data.Trees
{
    public class Tree
    {
        public PagePosition Root { get; }

        public Tree(PagePosition root)
        {
            Root = root;
        }

        public void AddClusterEntry(LowLevelTransaction tx, Span<byte> key, Span<byte> value)
        {
            var buffer = GetPageEntry(tx, key);
            if (buffer == null)
            {

            }
        }

        private PageBufferEntry GetRootEntry(LowLevelTransaction tx)
        {
            return tx.Buffers.GetEntry(Root);
        }

        private PageBufferEntry GetPageEntry(LowLevelTransaction tx, Span<byte> key, int level = -1)
        {
            if (level == 0)
            {
                return GetRootEntry(tx);
            }

            if (level == -1)
            {
                level = int.MaxValue;
            }

            var entry = GetRootEntry(tx);
            if (entry == null)
            {
                throw new InvalidOperationException($"can't find the tree's root page :{Root}");
            }

            var latchs = new Stack<LatchReleaserEntry>();
            var cursor = new TreePageCursor(entry.Page, TreeNodeFetchMode.LessThanOrEqual);

            latchs.Push(entry.Latch.EnterRead());

            for (; ; )
            {
                if (cursor.Level == level || cursor.IsLeaf)
                {
                    return tx.Buffers.GetEntry(cursor.Current.Position);
                }

                cursor = GetPageCursor(tx, cursor, key, TreeNodeFetchMode.LessThanOrEqual);
            }
        }

        private TreePageCursor GetPageCursor(LowLevelTransaction tx, TreePageCursor cursor, Span<byte> key, TreeNodeFetchMode mode)
        {
            if (cursor.IsLeaf)
            {
                throw new InvalidOperationException($"the current page is a leaf page,hav't the next level!");
            }
            else
            {
                cursor.Search(key);
            }

            if (!cursor.TryGetLastMatchedPageNumber(out var pageNumber))
            {
                throw new InvalidOperationException($"get next level page failed!");
            }

            var nextPage = tx.GetPage(Root.StorageId, pageNumber);
            if (nextPage != null)
            {
                return new TreePageCursor(nextPage, mode);
            }

            throw new NullReferenceException(nameof(nextPage));
        }
    }

    public ref struct TreeNodeEntry
    {
        public Span<byte> Key;

        public Span<byte> Value;
    }
}
