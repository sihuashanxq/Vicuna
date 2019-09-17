using System.Runtime.InteropServices;

namespace Vicuna.Engine.Data.Trees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct TreeRootHeader
    {
        public const int SizeOf = 12;

        [FieldOffset(0)]
        public int FileId;

        [FieldOffset(4)]
        public long PageNumber;
    }
}
