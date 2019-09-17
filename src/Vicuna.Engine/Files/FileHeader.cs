using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Vicuna.Engine.Storages
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Constants.PageSize)]
    public unsafe struct FileHeader
    {
        public const int SizeOf = Constants.PageHeaderSize;

        [FieldOffset(0)]
        public PageHeaderFlags Flags;

        [FieldOffset(1)]
        public int Id;

        [FieldOffset(5)]
        public long LSN;
    }
}
