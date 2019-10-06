using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Data.Trees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct TreePageHeader
    {
        internal const int SizeOf = Constants.PageHeaderSize;

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
        public ushort Low;

        [FieldOffset(39)]
        public ushort Upper;

        [FieldOffset(41)]
        public byte Depth;

        [FieldOffset(42)]
        public ushort Count;

        [FieldOffset(44)]
        public ushort UsedSize;

        [FieldOffset(46)]
        public TreeNodeFlags NodeFlags;

        [FieldOffset(47)]
        public long LastTransactionId;

        [FieldOffset(55)]
        public fixed byte Reserved[SizeOf - 55];

        public bool IsLeaf
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => NodeFlags.HasFlag(TreeNodeFlags.Leaf);
        }

        public bool IsRoot
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => NodeFlags.HasFlag(TreeNodeFlags.Root);
        }

        public bool IsBranch => !IsLeaf;

        public ushort FreeSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (ushort)(Constants.PageSize - UsedSize);
        }
    }
}
