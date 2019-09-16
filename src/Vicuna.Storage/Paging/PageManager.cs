using System;
using System.Collections.Generic;
using Vicuna.Engine.Storages;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Paging
{
    public class PageManager
    {
        public Dictionary<int, File> Files { get; }

        public PageManager(Dictionary<int, File> files)
        {
            Files = files ?? throw new ArgumentNullException(nameof(files));
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

        public virtual void Release()
        {

        }

        /// <summary>
        /// </summary>
        /// <param name="tx"></param>
        /// <param name="fileId"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public virtual List<PagePosition> AllocPageAtFree(LowLevelTransaction tx, int fileId, uint count)
        {
            return new List<PagePosition>();
        }

        /// <summary>
        /// </summary>
        /// <param name="tx"></param>
        /// <param name="fileId"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public virtual List<PagePosition> AllocPageAtFile(LowLevelTransaction tx, File file, uint count)
        {
            throw null;
            //if (file == null)
            //{
            //    throw new ArgumentNullException(nameof(file));
            //}

            //var list = new List<PagePosition>();
            //var size = count * Constants.PageSize;
            //var root = tx.ModifyPage(file.Root);
            //if (root == null)
            //{
            //    throw new NullReferenceException(nameof(root));
            //}

            //var last = root.Header.FileLength;
            //if (last + size > file.Length)
            //{
            //    file.AddLength(size);
            //}

            //for (var i = 0; i < count; i++)
            //{
            //    list.Add(new PagePosition(file.Id, last + i * Constants.PageSize));
            //}

            ////TODO:Log LastPageNumber
            //root.Header.FileLength += size;

            //return list;
        }
    }
}
