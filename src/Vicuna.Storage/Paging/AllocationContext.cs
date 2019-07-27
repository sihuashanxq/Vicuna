using Vicuna.Engine.Buffers;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Paging
{
    public struct AllocationContext
    {
        public uint Count { get; set; }

        public int FileId { get; set; }

        public AllocationMode Mode { get; set; }

        public BufferEntry RootBuffer { get; set; }

        public LowLevelTransaction Transaction { get; set; }
    }
}
