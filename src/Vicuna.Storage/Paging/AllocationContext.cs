using Vicuna.Storage.Buffers;

namespace Vicuna.Storage.Paging
{
    /// <summary>
    /// the context for allocate page
    /// </summary>
    public struct AllocationContext
    {
        /// <summary>
        /// alloate page's count
        /// </summary>
        public uint Count { get; set; }

        /// <summary>
        /// allocate mode
        /// </summary>
        public AllocationMode Mode { get; set; }

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
