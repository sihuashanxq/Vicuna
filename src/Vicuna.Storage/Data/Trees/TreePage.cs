using System;
using Vicuna.Storage.Paging;

namespace Vicuna.Storage.Data.Trees
{
    public class TreePage
    {
        internal Page Page;

        //internal ref PageTailer Tailer => ref Page.Tailer;

        //internal ref TreePageHeader Header => ref Page.Read<TreePageHeader>(0, TreePageHeader.SizeOf);

        public TreePage(Page page)
        {
            if (page == null)
            {
                throw new ArgumentNullException(nameof(page));
            }

            //if (page.Header.Flags != PageHeaderFlags.BTree)
            //{
            //    throw new ArgumentException($"page:{page.Header} is not a btree page!");
            //}

            Page = page;
        }
    }
}
