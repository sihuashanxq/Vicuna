using System;
using System.Collections.Generic;
using System.Threading;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Buffers
{
    public class PageBufferPool
    {
        public ReaderWriterLockSlim Mutex { get; }

        public PageBufferPoolOptions Options { get; }

        public PageBufferEntryLinkedList LRU { get; }

        public PageBufferEntryLinkedList Flush { get; }

        public Dictionary<PagePosition, PageBufferEntry> Hashes { get; }

        public PageManager PageManager { get; }

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

        public PageBufferEntry GetBufferEntry(int storeId, long pageNumber, PageBufferPeekFlags flags = PageBufferPeekFlags.None)
        {
            return GetBufferEntry(new PagePosition(storeId, pageNumber), flags);
        }

        public PageBufferEntry GetBufferEntry(PagePosition pos, PageBufferPeekFlags mode)
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

            if (!mode.HasFlag(PageBufferPeekFlags.NoneMoveLRU))
            {
                LRU.MoveToFirst(findEntry);
            }

            return findEntry;
        }

        private PageBufferEntry GetHashPageBufferEntry(PagePosition pos)
        {
            return Hashes.TryGetValue(pos, out var entry) ? entry : null;
        }

        private PageBufferEntry GetStorePageBufferEntry(PagePosition pos, PageBufferPeekFlags mode)
        {
            if (mode.HasFlag(PageBufferPeekFlags.NoneReading))
            {
                return null;
            }

            //loading from store
            return null;
        }
    }
}
