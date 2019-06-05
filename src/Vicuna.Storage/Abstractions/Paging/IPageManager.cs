using Vicuna.Storage.Abstractions.Stores;

namespace Vicuna.Storage.Abstractions.Paging
{
    public interface IPageManager
    {
        /// <summary>
        /// </summary>
        IStore Store { get; }

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
        /// <param name="pos"></param>
        void Free(PagePosition pos);

        /// <summary>
        /// </summary>
        /// <param name="storeId"></param>
        /// <returns></returns>
        PagePosition Allocate(int storeId);

        /// <summary>
        /// </summary>
        /// <param name="storeId"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        PagePosition[] Allocate(int storeId, uint count);
    }
}
