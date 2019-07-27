using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Vicuna.Engine.Storages;

namespace Vicuna.Engine.Paging
{
    public class PageManager
    {
        public IPageFreeHandler FreeHandler { get; }

        public Dictionary<int, File> Files { get; }

        public PageManager(Dictionary<int, File> stroages, IPageFreeHandler handler)
        {
            Files = stroages ?? throw new ArgumentNullException(nameof(stroages));
            FreeHandler = handler ?? throw new ArgumentNullException(nameof(handler));
        }

        protected virtual File GetFile(int id)
        {
            return Files.TryGetValue(id, out var store) ? store : null;
        }

        public virtual void WritePage(Page page)
        {
            if (page == null)
            {
                throw new ArgumentNullException(nameof(page));
            }

            var pos = page.Position;
            var file = GetFile(pos.FileId);
            if (file == null)
            {
                throw new KeyNotFoundException($" the file can not be found,id:{pos.FileId}!");
            }

            file.Write(pos.PageNumber, page.Data);
        }

        public virtual Page ReadPage(PagePosition pos)
        {
            var file = GetFile(pos.FileId);
            if (file == null)
            {
                throw new KeyNotFoundException($" the file can not be found,id:{pos.FileId}!");
            }

            var buffer = new byte[Constants.PageSize];

            file.Read(pos.PageNumber, buffer);

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
            var storage = GetFile(ctx.FileId);
            if (storage == null)
            {
                throw new KeyNotFoundException($"the storage can not be found,id:{ctx.FileId}!");
            }

            var index = 0;
            var positions = new PagePosition[ctx.Count];

            if (ctx.Mode == AllocationMode.None)
            {
                index = FreeHandler?.Allocate(ctx, positions) ?? 0;
            }

            if (ctx.Count == index)
            {
                return positions;
            }

            var first = Allocate(storage, (ctx.Count - index) * Constants.PageSize, ref ctx);
            if (first <= 0)
            {
                throw new InvalidOperationException($"failed to allocate pages ,file-id:{ctx.FileId},count:{ctx.Count - index}");
            }

            for (var i = index; i < positions.Length; i++)
            {
                positions[i] = new PagePosition(ctx.FileId, first);
                first += Constants.PageSize;
            }

            return positions;
        }

        /// <summary>
        /// </summary>
        /// <param name="storage"></param>
        /// <param name="size"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        private unsafe static long Allocate(File storage, long size, ref AllocationContext ctx)
        {
            var trx = ctx.Transaction;
            var journal = trx.Journal;

            var root = trx.ModifyPage(ctx.RootBuffer);
            ref var header = ref root.Header.Cast<FileHeader>();

            var last = header.LastPageNumber;
            if (last + size > header.FileLength)
            {
                header.FileLength = storage.AddLength(size);
                journal.WriteJournal(ctx.RootBuffer, header[nameof(header.FileLength)], header.FileLength);
            }

            header.LastPageNumber += size;
            journal.WriteJournal(ctx.RootBuffer, header[nameof(header.LastPageNumber)], header.LastPageNumber);

            return last;
        }
    }
}
