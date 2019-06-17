using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Vicuna.Engine.Storages
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct StorageHeader 
    {
        public const int SizeOf = 96;

        [FieldOffset(0)]
        public PageHeaderFlags Flags;

        [FieldOffset(1)]
        public int StoreId;

        [FieldOffset(5)]
        public long PageNumber;

        [FieldOffset(13)]
        public long LSN;

        [FieldOffset(21)]
        public long Length;

        [FieldOffset(29)]
        public long LastPageNumber;

        [FieldOffset(37)]
        public long RootPageNumber;

        [FieldOffset(45)]
        public long FreeRootPageNumber;

        [FieldOffset(37)]
        public fixed byte Reserved[SizeOf - 37 - 1];

        public ushort this[string name]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                switch (name)
                {
                    case nameof(Flags):
                        return 0;
                    case nameof(StoreId):
                        return 1;
                    case nameof(PageNumber):
                        return 5;
                    case nameof(LSN):
                        return 13;
                    case nameof(Length):
                        return 21;
                    case nameof(LastPageNumber):
                        return 29;
                    case nameof(RootPageNumber):
                        return 37;
                    case nameof(FreeRootPageNumber):
                        return 53;
                    default:
                        throw new InvalidOperationException($"invalid field name:{name}");
                }
            }
        }
    }
}
