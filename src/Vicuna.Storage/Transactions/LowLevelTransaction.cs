using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Transactions
{
    public class LowLevelTransaction
    {
        private readonly long _id;

        private readonly PageBufferPool _bufferPool;

        private readonly LowLevelTransactionJournal _journal;

        private readonly Stack<(ReadWriteLockType, IReadWriteLock)> _lockResources;

        private readonly Dictionary<PagePosition, ModifiedPageCache> _modifiedPages;

        public LowLevelTransaction(long id, PageBufferPool bufferPool)
        {
            _id = id;
            _bufferPool = bufferPool;
            _journal = new LowLevelTransactionJournal();
            _lockResources = new Stack<(ReadWriteLockType, IReadWriteLock)>();
            _modifiedPages = new Dictionary<PagePosition, ModifiedPageCache>();
        }

        public long Id => _id;

        public PageBufferPool BufferPool => _bufferPool;

        public LowLevelTransactionJournal Journal => _journal;

        public Page ModifyPage(PageBufferEntry buffer)
        {
            Debug.Assert(buffer.Page != null);
            Debug.Assert(buffer.Lock.IsWriteLockHeld);

            var modifies = _modifiedPages;
            if (!modifies.TryGetValue(buffer.Page.Position, out var cache))
            {
                cache = new ModifiedPageCache(buffer.Page, buffer.Page.CreateCopy());
                modifies[buffer.Page.Position] = cache;
            }

            return cache.Temporary;
        }

        public void AddLockResource(ReadWriteLockType lockType, IReadWriteLock readWriteLock)
        {
            _lockResources.Push((lockType, readWriteLock));
        }

        public void Reset()
        {
            _modifiedPages.Clear();
            _lockResources.Clear();
        }

        public void Commit()
        {
            CopyTemporaryToPages();
            ReleaseLockResources();
        }

        private void CopyTemporaryToPages()
        {
            foreach (var item in _modifiedPages)
            {
                var page = item.Value.Page;
                var temporary = item.Value.Temporary;

                temporary.CopyTo(page);
            }
        }

        private void ReleaseLockResources()
        {
            while (_lockResources.Count != 0)
            {
                var (lockType, lockEntry) = _lockResources.Pop();

                switch (lockType)
                {
                    case ReadWriteLockType.Read:
                        lockEntry.ExitReadLock();
                        break;
                    case ReadWriteLockType.Write:
                        lockEntry.ExitWriteLock();
                        break;
                }
            }
        }

        private struct ModifiedPageCache
        {
            public Page Page;

            public Page Temporary;

            public ModifiedPageCache(Page page, Page temporary)
            {
                Page = page;
                Temporary = temporary;
            }
        }
    }
}
