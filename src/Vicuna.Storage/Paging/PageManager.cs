using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Vicuna.Engine.Storages;

namespace Vicuna.Engine.Paging
{
    public class PageManager
    {
        public IPageFreeHandler FreeHandler { get; }

        public Dictionary<int, Storage> Stroages { get; }

        public PageManager(Dictionary<int, Storage> stroages, IPageFreeHandler handler)
        {
            Stroages = stroages ?? throw new ArgumentNullException(nameof(stroages));
            FreeHandler = handler ?? throw new ArgumentNullException(nameof(handler));
        }

        protected virtual Storage GetStorage(int id)
        {
            return Stroages.TryGetValue(id, out var store) ? store : null;
        }

        public virtual void SetPage(Page page)
        {
            if (page == null)
            {
                throw new ArgumentNullException(nameof(page));
            }

            var position = page.Position;
            var storage = GetStorage(position.StorageId);
            if (storage == null)
            {
                throw new KeyNotFoundException($" the store can not be found,id:{position.StorageId}!");
            }

            storage.Write(position.PageNumber, page.Data);
        }

        public virtual Page GetPage(PagePosition pos)
        {
            var store = GetStorage(pos.StorageId);
            if (store == null)
            {
                throw new KeyNotFoundException($" the store can not be found,id:{pos.StorageId}!");
            }

            var buffer = new byte[Constants.PageSize];

            store.Read(pos.PageNumber, buffer);

            return new Page(buffer);
        }

        public virtual void Release(ref ReleaseContext ctx)
        {
            if (FreeHandler != null)
            {
                FreeHandler.Release(ctx);
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        public virtual PagePosition[] Allocate(ref AllocationContext ctx)
        {
            var storage = GetStorage(ctx.StorageId);
            if (storage == null)
            {
                throw new KeyNotFoundException($"the storage can not be found,id:{ctx.StorageId}!");
            }

            var index = 0;
            var positions = new PagePosition[ctx.Count];

            if (ctx.Mode == AllocationMode.Normal)
            {
                index = FreeHandler?.Allocate(ctx, positions) ?? 0;
            }

            if (ctx.Count == index)
            {
                return positions;
            }

            var pageNumber = Allocate(storage, (ctx.Count - index) * Constants.PageSize, ref ctx);
            if (pageNumber <= 0)
            {
                throw new InvalidOperationException($"failed to allocate pages ,store-id:{ctx.StorageId},count:{ctx.Count - index}");
            }

            for (var i = index; i < positions.Length; i++)
            {
                positions[i] = new PagePosition(ctx.StorageId, pageNumber);
                pageNumber += Constants.PageSize;
            }

            return positions;
        }

        /// <summary>
        /// </summary>
        /// <param name="storage"></param>
        /// <param name="size"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        private unsafe static long Allocate(Storage storage, long size, ref AllocationContext ctx)
        {
            var trx = ctx.Transaction;
            var journal = trx.Journal;

            //enter lock and hold it
            ctx.StorageRootEntry.Lock.EnterWriteLock();
            trx.AddLockReleaser(ReadWriteLockType.Write, ctx.StorageRootEntry.Lock);

            var rootPage = trx.ModifyPage(ctx.StorageRootEntry);
            ref var header = ref rootPage.Header.Cast<StorageHeader>();

            var last = header.LastPageNumber;
            if (last + size > header.StorageLength)
            {
                header.StorageLength = storage.AddLength(size);
                journal.WriteJournal(ctx.StorageRootEntry, header[nameof(header.StorageLength)], header.StorageLength);
            }

            header.LastPageNumber += size;
            journal.WriteJournal(ctx.StorageRootEntry, header[nameof(header.LastPageNumber)], header.LastPageNumber);

            return last;
        }
    }
}
