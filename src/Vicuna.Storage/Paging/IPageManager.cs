using System.Collections.Generic;
using Vicuna.Storage.Stores;

namespace Vicuna.Storage.Paging
{
    public interface IPageManager
    {
        /// <summary>
        /// </summary>
        IPageFreeHandler FreeHandler { get; }

        /// <summary>
        /// </summary>
        Dictionary<int, IStore> Stores { get; }

        /// <summary>
        /// </summary>
        /// <param name="page"></param>
        void SetPage(Page page);

        /// <summary>
        /// </summary>
        /// <param name="pos"></param>
        /// <returns></returns>
        Page GetPage(PagePosition pos);

        /// <summary>
        /// </summary>
        /// <param name="page"></param>
        void Release(ref ReleaseContext ctx);

        /// <summary>
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        PagePosition[] Allocate(ref AllocationContext ctx);
    }
}
