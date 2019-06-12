using Vicuna.Storage.Buffers;
using Vicuna.Storage.Journal;

namespace Vicuna.Storage.Transactions
{
    public interface ILowLevelTransaction
    {
        void WriteJournalLog(PageBufferEntry entry, JournalFlags flags, byte[] buffer);
    }
}
