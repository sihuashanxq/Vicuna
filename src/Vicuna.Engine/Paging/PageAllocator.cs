using System.Collections.Generic;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Paging
{
    public class PageAllocator
    {
        public void Release(LowLevelTransaction lltx, PagePosition page)
        {

        }

        public IEnumerable<PagePosition> Alloc(LowLevelTransaction lltx, int fileId, int count)
        {
            throw null;
        }
    }
}
