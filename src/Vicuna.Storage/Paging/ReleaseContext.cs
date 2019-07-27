using Vicuna.Engine.Buffers;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Paging
{
    public struct ReleaseContext
    {
        public Page[] Pages { get; set; }

        public BufferEntry RootBuffer { get; set; }

        public LowLevelTransaction Transaction { get; set; }
    }
}
