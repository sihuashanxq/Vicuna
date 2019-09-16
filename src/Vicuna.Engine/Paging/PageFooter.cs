using System.Runtime.InteropServices;

namespace Vicuna.Engine.Paging
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct PageFooter
    {
        internal const int SizeOf = Constants.PageFooterSize;

        [FieldOffset(0)]
        public long LSN;
    }
}
