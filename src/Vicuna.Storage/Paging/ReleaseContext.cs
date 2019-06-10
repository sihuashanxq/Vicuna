using Vicuna.Storage.Buffers;

namespace Vicuna.Storage.Paging
{
    /// <summary>
    /// the context to release page
    /// </summary>
    public struct ReleaseContext
    {
        /// <summary>
        /// the pages will be released
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

        public ref PageStoreRootHeader RootHeader
           => ref RootEntry.Page.GetHeader<PageStoreRootHeader>(PageStoreRootHeader.SizeOf);
    }
}
