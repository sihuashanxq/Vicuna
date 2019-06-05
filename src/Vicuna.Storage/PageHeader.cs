using System.Runtime.InteropServices;

namespace Vicuna.Storage
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct PageHeader
    {
        public const int SizeOf = sizeof(PageHeaderFlags) + sizeof(int) + sizeof(long) + sizeof(long);

        [FieldOffset(0)]
        public PageHeaderFlags Flags;

        [FieldOffset(1)]
        public int StoreId;

        [FieldOffset(5)]
        public long PageNumber;

        [FieldOffset(13)]
        public long LSN;
    }
}
