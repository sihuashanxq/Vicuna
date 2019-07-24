using System;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;
using Vicuna.Engine.Buffers;

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

            using (var trans = tx.CopyNew())
            {
                var entry = GetRootEntry(tx);
                if (entry == null)
                {
                    throw new InvalidOperationException($"can't find the tree's root page :{Root}");
                }

                entry.Lock.EnterReadLock();
                trans.PushLockWaitForRelease(ReadWriteLockType.Read, entry.Lock);

                var cursor = new TreePageCursor(entry.Page, TreeNodeFetchMode.LessThanOrEqual);

                for (; ; )
                {
                    if (cursor.Level == level || cursor.IsLeaf)
                    {
                        return trans.Buffers.GetEntry(cursor.Current.Position);
                    }

                    cursor = GetKeyPageCursor(trans, cursor, key, TreeNodeFetchMode.LessThanOrEqual);
                }
            }
        }

        private TreePageCursor GetKeyPageCursor(LowLevelTransaction tx, TreePageCursor cursor, Span<byte> key, TreeNodeFetchMode mode)
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

            var nextLevelPage = tx.GetPage(Root.StorageId, pageNumber);
            if (nextLevelPage != null)
            {
                return new TreePageCursor(nextLevelPage, mode);
            }

            throw new NullReferenceException(nameof(nextLevelPage));
        }
    }

    public ref struct TreeNodeEntry
    {
        public Span<byte> Key;

        public Span<byte> Value;
    }
}
