using System.Runtime.InteropServices;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct FixedSizeTreeRootHeader
    {
        public const int SizeOf = 13;

        [FieldOffset(0)]
        public int FileId;

        [FieldOffset(4)]
        public long PageNumber;

        [FieldOffset(12)]
        public byte DataElementSize;

        public override int GetHashCode()
        {
            var hash = FileId.GetHashCode();

            hash = hash * 31 ^ PageNumber.GetHashCode();

            return hash;
        }

        public override bool Equals(object obj)
        {
            if (obj is FixedSizeTreeRootHeader root)
            {
                return FileId == root.FileId && PageNumber == root.PageNumber;
            }

            return false;
        }
    }
}