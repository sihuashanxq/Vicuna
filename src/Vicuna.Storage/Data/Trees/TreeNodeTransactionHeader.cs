using System.Runtime.InteropServices;

namespace Vicuna.Engine.Data.Trees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct TreeNodeTransactionHeader
    {
        public const ushort SizeOf = sizeof(long) + sizeof(long);

        [FieldOffset(0)]
        public long TransactionNumber;

        [FieldOffset(8)]
        public long TransactionRollbackNumber;
    }
}
