using Vicuna.Engine.Buffers;
using Vicuna.Engine.Transactions;
using Vicuna.Engine.Storages;
namespace Vicuna.Engine.Paging
{
    public struct ReleaseContext
    {
        public Page[] Pages { get; set; }

        public LowLevelTransaction Transaction { get; set; }

        public PageBufferEntry StorageRootEntry { get; set; }
    }
}
