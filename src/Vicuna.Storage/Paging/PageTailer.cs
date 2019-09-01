using System.Runtime.InteropServices;

namespace Vicuna.Engine.Paging
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct PageTailer
    {
        internal const int SizeOf = Constants.PageTailerSize;

        [FieldOffset(0)]
        public long LSN;
    }
}
