using System;

namespace Vicuna.Engine.Paging
{
    public class PageDamageException : Exception
    {
        public PageDamageException(Page page) : base($"the page: {page.Position} was damaged!")
        {

        }
    }
}
