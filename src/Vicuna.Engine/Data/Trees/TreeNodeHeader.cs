using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Vicuna.Engine.Data.Trees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct TreeNodeHeader
    {
        public const ushort SizeOf = 12;

        public const ushort SlotSize = sizeof(ushort);

        [FieldOffset(0)]
        public bool IsDeleted;

        [FieldOffset(1)]
        public ushort KeySize;

        [FieldOffset(3)]
        public ushort DataSize;

        [FieldOffset(3)]
        public long PageNumber;

        [FieldOffset(11)]
        public TreeNodeHeaderFlags NodeFlags;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetSize()
        {
            switch (NodeFlags)
            {
                case TreeNodeHeaderFlags.Primary:
                    return (ushort)(SizeOf + SlotSize + KeySize + DataSize + TreeNodeTransactionHeader.SizeOf);
                case TreeNodeHeaderFlags.Data:
                    return (ushort)(SizeOf + SlotSize + KeySize + DataSize);
                default:
                    return (ushort)(SizeOf + SlotSize + KeySize);
            }
        }
    }
}
