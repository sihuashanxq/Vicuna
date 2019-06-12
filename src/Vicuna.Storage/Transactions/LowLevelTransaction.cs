using System.Collections.Generic;
using Vicuna.Storage.Buffers;
using Vicuna.Storage.Journal;

namespace Vicuna.Storage.Transactions
{
    public class LowLevelTransaction : ILowLevelTransaction
    {
        private readonly List<byte> _journalBuffer = new List<byte>();

        private readonly HashSet<PageBufferEntry> _releaseEntries;

        private readonly HashSet<PageBufferEntry> _modifiedEntries;

        public void Commit()
        {

        }

        public void WriteJournalLog(PageBufferEntry entry, JournalFlags flags, byte[] buffer)
        {
            throw new System.NotImplementedException();
        }
    }
}
