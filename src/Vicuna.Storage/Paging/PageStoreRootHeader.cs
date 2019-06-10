using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Vicuna.Storage.Paging
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct PageStoreRootHeader : IPageHeader
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

        [FieldOffset(53)]
        public fixed byte Reserved[SizeOf - 53 - 1];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Offset(string name)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLSN()
        {
            return LSN;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetSizeOf()
        {
            return SizeOf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetStoreId()
        {
            return StoreId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPageNumber()
        {
            return PageNumber;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PageHeaderFlags GetFlags()
        {
            return Flags;
        }
    }
}
