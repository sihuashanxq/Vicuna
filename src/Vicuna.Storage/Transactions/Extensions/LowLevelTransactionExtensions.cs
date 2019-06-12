using Vicuna.Storage.Buffers;
using Vicuna.Storage.Journal;

namespace Vicuna.Storage.Transactions.Extensions
{
    public unsafe static class LowLevelTransactionExtensions
    {
        public static void WriteSetByte8JournalLog(this ILowLevelTransaction tx, PageBufferEntry entry, short offset, long value)
        {
            var buffer = new byte[sizeof(short) + sizeof(long)];

            fixed (byte* ptr = buffer)
            {
                *((short*)ptr) = offset;
                *((long*)&ptr[sizeof(short)]) = value;
            }

            tx.WriteJournalLog(entry, JournalFlags.SetByte8, buffer);
        }
    }
}
