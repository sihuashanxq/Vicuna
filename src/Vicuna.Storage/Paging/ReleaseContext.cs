using Vicuna.Storage.Buffers;
using Vicuna.Storage.Transactions;

namespace Vicuna.Storage.Paging
{
    /// <summary>
    /// page release context
    /// </summary>
    public struct ReleaseContext
    {
        /// <summary>
        /// the pages to be released
        /// </summary>
        public Page[] Pages { get; set; }

        /// <summary>
        /// the store's root page buffer entry
        /// </summary>
        public PageBufferEntry RootEntry { get; set; }

        /// <summary>
        /// tx
        /// </summary>
        public ILowLevelTransaction Transaction { get; set; }

        public ref PagedStoreRootHeader RootHeader
           => ref RootEntry.Page.GetHeader<PagedStoreRootHeader>(PagedStoreRootHeader.SizeOf);
    }
}
