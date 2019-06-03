using System.Runtime.InteropServices;

namespace Vicuna.Storage.Data.Trees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct TreeNodeTransactionHeader
    {
        public const ushort SizeOf = 16;

        [FieldOffset(0)]
        public long TransactionNumber;

        [FieldOffset(8)]
        public long TransactionRollbackNumber;
    }
}
