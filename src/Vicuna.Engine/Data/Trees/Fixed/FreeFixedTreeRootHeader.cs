using System.Runtime.InteropServices;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 13)]
    public struct FreeFixedTreeRootHeader
    {
        [FieldOffset(0)]
        public int FileId;

        [FieldOffset(4)]
        public long PageNumber;

        [FieldOffset(12)]
        public byte DataElementSize;
    }
}
