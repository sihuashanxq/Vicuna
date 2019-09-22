using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Vicuna.Engine.Data.Trees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct TreeNodeHeader
    {
        public const ushort SizeOf = 12;

        [FieldOffset(0)]
        public bool IsDeleted;

        [FieldOffset(1)]
        public ushort KeySize;

        [FieldOffset(3)]
        public long PageNumber;

        [FieldOffset(9)]
        public ushort DataSize;

        [FieldOffset(11)]
        public TreeNodeHeaderFlags NodeFlags;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetSize()
        {
            var hasValue = NodeFlags.HasValue();
            var hasVersion = NodeFlags.HasVersion();
            if (hasVersion)
            {
                return (ushort)(SizeOf + sizeof(short) + KeySize + (hasValue ? DataSize : 0) + TreeNodeVersionHeader.SizeOf);
            }

            return (ushort)(SizeOf + sizeof(short) + KeySize + (hasValue ? DataSize : 0));
        }
    }
}
