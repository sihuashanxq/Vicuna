using System;
using System.Collections.Generic;
using System.Threading;
using Vicuna.Storage.Paging;

namespace Vicuna.Storage.Buffers
{
    public class PageBufferPool
    {
        public ReaderWriterLockSlim Mutex { get; }

        public PageBufferPoolOptions Options { get; }

        public PageBufferEntryLinkedList LRU { get; }

        public PageBufferEntryLinkedList Flush { get; }

        public Dictionary<PagePosition, PageBufferEntry> Hashes { get; }

        public IPageManager PageManager { get; }

        public PageBufferPool(PageBufferPoolOptions options)
        {
            Options = options;
            LRU = new PageBufferEntryLinkedList();
            Flush = new PageBufferEntryLinkedList();
            Mutex = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            Hashes = new Dictionary<PagePosition, PageBufferEntry>();
        }

        public void AddFlushBufferEntry(PageBufferEntry entry)
        {
            Flush.AddFirst(entry);
        }

        public PageBufferEntry GetBufferEntry(PagePosition pos, PageBufferPoolFetchMode mode)
        {
            var findEntry = GetHashPageBufferEntry(pos);
            if (findEntry == null)
            {
                findEntry = GetStorePageBufferEntry(pos, mode);
            }

            if (findEntry == null)
            {
                return findEntry;
            }

            if (!mode.HasFlag(PageBufferPoolFetchMode.NoFlushLRU))
            {
                LRU.MoveToFirst(findEntry);
            }

            findEntry.LastTicks = DateTime.UtcNow.Ticks;

            return findEntry;
        }

        private PageBufferEntry GetHashPageBufferEntry(PagePosition pos)
        {
            return Hashes.TryGetValue(pos, out var entry) ? entry : null;
        }

        private PageBufferEntry GetStorePageBufferEntry(PagePosition pos, PageBufferPoolFetchMode mode)
        {
            if (mode.HasFlag(PageBufferPoolFetchMode.JustBufferPool))
            {
                return null;
            }

            //loading from store
            return null;
        }
    }
}
