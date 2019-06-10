using System;
using System.Collections.Generic;
using Vicuna.Storage.Stores;
using Vicuna.Storage.Buffers;

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

        public virtual void Release(ReleaseContext ctx)
        {
            if (Handler != null)
            {
                Handler.Release(ctx);
            }
        }

        public virtual PagePosition[] Allocate(AllocationContext ctx)
        {
            var id = ctx.RootHeader.GetStoreId();
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
                var size = (ctx.Count - index) * Constants.PageSize;
                var pageNumber = Allocate(store, size, ref ctx.RootHeader);
                if (pageNumber <= 0)
                {
                    throw new InvalidOperationException($"allocate page failed,storeId:{id}");
                }

                for (var i = index; i < pages.Length; i++)
                {
                    pages[i] = new PagePosition(id, pageNumber * Constants.PageSize);
                    pageNumber++;
                }

                ctx.Transaction.AppendJournalStoreAllcates(ctx.RootEntry, ctx.RootHeader.Length, ctx.RootHeader.LastPageNumber);
            }

            return pages;
        }

        /// <summary>
        /// </summary>
        /// <param name="store"></param>
        /// <param name="size"></param>
        /// <param name="root"></param>
        /// <returns></returns>
        private static long Allocate(IStore store, long size, ref PageStoreRootHeader root)
        {
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
            return last;
        }
    }

    public interface ILowLevelTransaction
    {
        void AppendJournalStoreAllcates(PageBufferEntry entry, long length, long lastPageNumber);
    }
}
