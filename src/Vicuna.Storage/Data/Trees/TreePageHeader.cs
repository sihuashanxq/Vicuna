using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct TreePageHeader 
    {
        internal const int SizeOf = 96;

        [FieldOffset(0)]
        public PageHeaderFlags Flags;

        [FieldOffset(1)]
        public int StoreId;

        [FieldOffset(5)]
        public long PageNumber;

        [FieldOffset(13)]
        public long LSN;

        [FieldOffset(21)]
        public byte Level;

        [FieldOffset(22)]
        public long PrevPageNumber;

        [FieldOffset(30)]
        public long NextPageNumber;

        [FieldOffset(38)]
        public ushort Low;

        [FieldOffset(40)]
        public ushort Upper;

        [FieldOffset(42)]
        public ushort Count;

        [FieldOffset(44)]
        public ushort UsedSize;

        [FieldOffset(46)]
        public TreeNodeFlags NodeFlags;

        [FieldOffset(47)]
        public long LastTransactionId;

        [FieldOffset(55)]
        public fixed byte Reserved[SizeOf - 55 - 1];
    }
}
