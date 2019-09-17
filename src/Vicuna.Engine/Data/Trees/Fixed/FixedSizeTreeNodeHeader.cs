using System.Runtime.InteropServices;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    [StructLayout(LayoutKind.Explicit, Size = SizeOf)]
    public struct FixedSizeTreeNodeHeader
    {
        public const int SizeOf = 8;

        [FieldOffset(0)]
        public long PageNumber;
    }
}
