using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Storages
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct FileHeader
    {
        public const int SizeOf = Constants.PageHeaderSize;

        [FieldOffset(0)]
        public PageHeaderFlags Flags;

        [FieldOffset(1)]
        public int Id;

        [FieldOffset(5)]
        public long LSN;

        [FieldOffset(13)]
        public long FileLength;

        [FieldOffset(21)]
        public long LastPageNumber;

        public ushort this[string name]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                switch (name)
                {
                    case nameof(Flags):
                        return 0;
                    case nameof(Id):
                        return 1;
                    case nameof(LSN):
                        return 5;
                    case nameof(FileLength):
                        return 13;
                    case nameof(LastPageNumber):
                        return 21;
                    default:
                        throw new InvalidOperationException($"invalid field name:{name}");
                }
            }
        }
    }
}
