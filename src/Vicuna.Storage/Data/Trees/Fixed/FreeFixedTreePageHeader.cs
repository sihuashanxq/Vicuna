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
        public byte Depth;

        [FieldOffset(40)]
        public byte DataElementSize;

        [FieldOffset(41)]
        public TreeNodeFlags NodeFlags;

        [FieldOffset(42)]
        public fixed byte Reserved[SizeOf - 42];

        public static short Offset(string name)
        {
            switch (name)
            {
                case nameof(Flags):
                    return 0;
                case nameof(FileId):
                    return 1;
                case nameof(PageNumber):
                    return 5;
                case nameof(LSN):
                    return 13;
                case nameof(PrevPageNumber):
                    return 21;
                case nameof(NextPageNumber):
                    return 29;
                case nameof(Count):
                    return 37;
                case nameof(Depth):
                    return 39;
                case nameof(DataElementSize):
                    return 40;
                case nameof(NodeFlags):
                    return 41;
                default:
                    return -1;
            }
        }
    }
}
