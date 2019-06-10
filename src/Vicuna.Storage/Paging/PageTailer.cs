using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Vicuna.Storage.Paging
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct PageTailer : IPageTailer
    {
        internal const int SizeOf = sizeof(long);

        [FieldOffset(0)]
        public long LSN;

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
    }
}
