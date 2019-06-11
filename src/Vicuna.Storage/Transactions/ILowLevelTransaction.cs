using System.Collections.Generic;
using Vicuna.Storage.Buffers;
using Vicuna.Storage.Journal;

namespace Vicuna.Storage.Transactions
{
    public interface ILowLevelTransaction
    {
        void WriteJournal(PageBufferEntry entry, JournlaFlags flags, byte[] buffer);
    }
}
