using System;

namespace Vicuna.Engine.Paging
{
    public class PageCorruptedException : Exception
    {
        public PageCorruptedException(Page page) : base($"the page at {page.Position} was corrupted!")
        {

        }
    }
}
