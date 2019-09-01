using System.Runtime.InteropServices;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct FreeFixedTreePageHeader
    {
        internal const int SizeOf = 96;

        [FieldOffset(0)]
        public PageHeaderFlags Flags;

        [FieldOffset(1)]
        public int FileId;

        [FieldOffset(5)]
        public long PageNumber;

        [FieldOffset(13)]
        public long LSN;

        [FieldOffset(21)]
        public long PrevPageNumber;

        [FieldOffset(29)]
        public long NextPageNumber;

        [FieldOffset(37)]
        public ushort Count;

        [FieldOffset(39)]
        public byte DataElementSize;

        [FieldOffset(40)]
        public TreeNodeFlags NodeFlags;

        [FieldOffset(41)]
        public fixed byte Reserved[SizeOf - 41];
    }
}
