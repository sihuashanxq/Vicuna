using System.Runtime.InteropServices;

namespace Vicuna.Engine.Paging
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Constants.PageHeaderSize)]
    public unsafe struct PageHeader
    {
        public const int SizeOf = Constants.PageHeaderSize;

        [FieldOffset(0)]
        public PageHeaderFlags Flags;

        [FieldOffset(1)]
        public int FileId;

        [FieldOffset(5)]
        public long PageNumber;

        [FieldOffset(13)]
        public long LSN;
    }
}
