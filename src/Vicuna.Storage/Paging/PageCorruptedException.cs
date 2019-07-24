using System;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Paging
{
    public class PageCorruptedException : Exception
    {
        public PageCorruptedException(Page page) : base($"page data corrupted,{page}!")
        {

        }
    }
}
