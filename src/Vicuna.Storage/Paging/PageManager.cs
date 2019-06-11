using System;
using System.Collections.Generic;
using Vicuna.Storage.Journal;
using Vicuna.Storage.Stores;

namespace Vicuna.Storage.Paging
{
    public class PageManager : IPageManager
    {
        public IPageFreeHandler Handler { get; }

        public Dictionary<int, IStore> Stores { get; }

        public PageManager(Dictionary<int, IStore> stores, IPageFreeHandler handler)
        {
            Stores = stores ?? throw new ArgumentNullException(nameof(stores));
            Handler = handler ?? throw new ArgumentNullException(nameof(handler));
        }

        protected virtual IStore GetStore(int id)
        {
            return Stores.TryGetValue(id, out var store) ? store : null;
        }

        public virtual void SetPage(Page page)
        {
            if (page == null)
            {
                throw new ArgumentNullException(nameof(page));
            }

            var pos = page.Position;
            if (pos.StoreId < 0)
            {
                throw new ArgumentException($"page's store id{pos.StoreId} is invalid");
            }

            if (pos.PageNumber < 0)
            {
                throw new ArgumentException($"page's store page-number{pos.PageNumber} is invalid");
            }

            var store = GetStore(pos.StoreId);
            if (store == null)
            {
                throw new KeyNotFoundException($" the store can not be found,id:{pos.StoreId}!");
            }

            lock (store.SyncRoot)
            {
                store.Write(pos.PageNumber, page.Data);
            }
        }

        public virtual Page GetPage(PagePosition pos)
        {
            var store = GetStore(pos.StoreId);
            if (store == null)
            {
                throw new KeyNotFoundException($" the store can not be found,id:{pos.StoreId}!");
            }

            var buffer = new byte[Constants.PageSize];

            lock (store.SyncRoot)
            {
                store.Read(pos.PageNumber, buffer);
            }

            return new Page(buffer);
        }

        public virtual void Release(ref ReleaseContext ctx)
        {
            if (Handler != null)
            {
                Handler.Release(ctx);
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        public virtual PagePosition[] Allocate(ref AllocationContext ctx)
        {
            var id = ctx.RootHeader.StoreId;
            if (id < 0)
            {
                throw new ArgumentException($"store's id invalid!");
            }

            var store = GetStore(id);
            if (store == null)
            {
                throw new KeyNotFoundException($"the store can not be found,id:{id}!");
            }

            var index = 0;
            var pages = new PagePosition[ctx.Count];

            if (ctx.Mode == AllocationMode.Normal)
            {
                index = Handler?.Allocate(ctx, pages) ?? 0;
            }

            if (ctx.Count == index)
            {
                return pages;
            }

            using (ctx.RootEntry.EnterLock(LockMode.X_LOCK))
            {
                var pageNumber = Allocate(store, (ctx.Count - index) * Constants.PageSize, ref ctx);
                if (pageNumber <= 0)
                {
                    throw new InvalidOperationException($"failed to allocate pages,storeId:{id}");
                }

                for (var i = index; i < pages.Length; i++)
                {
                    pages[i] = new PagePosition(id, pageNumber);
                    pageNumber += Constants.PageSize;
                }
            }

            return pages;
        }

        /// <summary>
        /// </summary>
        /// <param name="store"></param>
        /// <param name="size"></param>
        /// <param name="root"></param>
        /// <returns></returns>
        private static long Allocate(IStore store, long size, ref AllocationContext ctx)
        {
            ref var root = ref ctx.RootHeader;
            var last = root.LastPageNumber;
            if (size > root.Length - root.LastPageNumber)
            {
                lock (store.SyncRoot)
                {
                    store.AddLength(size);
                    root.Length += size;
                }
            }

            root.LastPageNumber += size;
            WriteAllocatePagesJournal(ref ctx);

            return last;
        }

        /// <summary>
        /// </summary>
        /// <param name=""></param>
        private static void WriteAllocatePagesJournal(ref AllocationContext ctx)
        {
            var journal = new byte[16 + 2];
            var buffer = (Span<byte>)journal;
            var offset = PagedStoreRootHeader.Offset(nameof(ctx.RootHeader.Length));

            //offset|Length|LastPageNumber
            BitConverter.TryWriteBytes(buffer.Slice(0, 2), offset);
            BitConverter.TryWriteBytes(buffer.Slice(2, 8), ctx.RootHeader.Length);
            BitConverter.TryWriteBytes(buffer.Slice(10, 8), ctx.RootHeader.LastPageNumber);

            ctx.Transaction.WriteJournal(ctx.RootEntry, JournlaFlags.Set_Byte16, journal);
        }
    }
}
