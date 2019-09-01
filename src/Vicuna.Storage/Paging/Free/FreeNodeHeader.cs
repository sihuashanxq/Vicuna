using System.Runtime.InteropServices;

namespace Vicuna.Engine.Paging.Free
{
    [StructLayout(LayoutKind.Explicit, Size = SizeOf)]
    public struct FreeNodeHeader
    {
        const int SizeOf = 14;

        [FieldOffset(0)]
        public int FileId;

        [FieldOffset(4)]
        public long PageNumber;

        [FieldOffset(12)]
        public ushort Size;
    }
}
