using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Vicuna.Storage.Paging
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public unsafe struct PageHeader : IPageHeader
    {
        public const int SizeOf = sizeof(PageHeaderFlags) + sizeof(int) + sizeof(long) + sizeof(long);

        [FieldOffset(0)]
        public PageHeaderFlags Flags;

        [FieldOffset(1)]
        public int StoreId;

        [FieldOffset(5)]
        public long PageNumber;

        [FieldOffset(13)]
        public long LSN;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLSN()
        {
            return LSN;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetStoreId()
        {
            return StoreId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetSizeOf()
        {
            return SizeOf;
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
