using System;
using Vicuna.Storage.Buffers;
using Vicuna.Storage.Journal;
using Vicuna.Storage.Transactions;

namespace Vicuna.Storage.Paging
{
    /// <summary>
    /// page allocation context
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

        public ref PagedStoreRootHeader RootHeader
            => ref RootEntry.Page.GetHeader<PagedStoreRootHeader>(PagedStoreRootHeader.SizeOf);
    }
}
