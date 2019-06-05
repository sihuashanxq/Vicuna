using System;
using Vicuna.Storage.Abstractions.Paging;
using Vicuna.Storage.Abstractions.Stores;

namespace Vicuna.Storage.Paging
{
    public class PageManager : IPageManager
    {
        public IStore Store { get; }

        public PageManager(IStore store)
        {
            Store = store ?? throw new ArgumentNullException(nameof(store));
        }

        public void Free(PagePosition pos)
        {

        }

        public Page GetPage(PagePosition pos)
        {
            throw new NotImplementedException();
        }

        public void SetPage(Page page)
        {
            throw new NotImplementedException();
        }

        public PagePosition Allocate(int storeId)
        {
            throw new NotImplementedException();
        }

        public PagePosition[] Allocate(int storeId, uint count)
        {
            throw new NotImplementedException();
        }
    }
}
