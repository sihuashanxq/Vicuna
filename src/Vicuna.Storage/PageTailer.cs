using System.Runtime.InteropServices;

namespace Vicuna.Storage
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct PageTailer
    {
        internal const int SizeOf = sizeof(long);

        [FieldOffset(0)]
        public long LSN;
    }
}
