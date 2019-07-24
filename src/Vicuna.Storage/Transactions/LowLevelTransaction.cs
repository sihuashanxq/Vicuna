using System;
using System.Collections.Generic;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Transactions
{
    public class LowLevelTransaction : IDisposable
    {
        private readonly long _id;

        private readonly bool _writeable;

        private readonly PageBufferPool _bufferPool;

        private readonly LowLevelTransactionJournal _journal;

        private readonly Dictionary<PagePosition, ModifyBufferCache> _modifidBuffers;

        private readonly Stack<(ReadWriteLockType, IReadWriteLock)> _txLocksWaitForRelease;

        public long Id => _id;

        public PageBufferPool Buffers => _bufferPool;

        public LowLevelTransactionJournal Journal => _journal;

        internal Dictionary<PagePosition, ModifyBufferCache> ModifidBuffers => _modifidBuffers;

        public LowLevelTransaction CopyNew()
        {
            return new LowLevelTransaction(Id, Buffers);
        }

        public LowLevelTransaction(long id, PageBufferPool bufferPool)
        {
            _id = id;
            _bufferPool = bufferPool;
            _journal = new LowLevelTransactionJournal();
            _modifidBuffers = new Dictionary<PagePosition, ModifyBufferCache>();
            _txLocksWaitForRelease = new Stack<(ReadWriteLockType, IReadWriteLock)>();
        }

        public Page GetPage(int id, long number)
        {
            return GetPage(new PagePosition(id, number));
        }

        public Page GetPage(PagePosition pos)
        {
            var temporary = GetModifiedTemporaryPage(pos);
            if (temporary != null)
            {
                return temporary;
            }

            var buffer = Buffers.GetEntry(pos);
            if (buffer == null)
            {
                return null;
            }

            return GetPage(buffer);
        }

        public Page GetPage(PageBufferEntry buffer)
        {
            var temporary = GetModifiedTemporaryPage(buffer.Position);
            if (temporary != null)
            {
                return null;
            }

            buffer.Lock.EnterReadLock();
            PushLockWaitForRelease(ReadWriteLockType.Read, buffer.Lock);

            return buffer.Page;
        }

        public Page ModifyPage(PagePosition pos)
        {
            var temporary = GetModifiedTemporaryPage(pos);
            if (temporary != null)
            {
                return temporary;
            }

            var buffer = Buffers.GetEntry(pos);
            if (buffer == null)
            {
                return null;
            }

            buffer.Lock.EnterWriteLock();
            temporary = buffer.Page.CreateCopy();

            ModifidBuffers[buffer.Position] = new ModifyBufferCache(buffer, temporary);
            PushLockWaitForRelease(ReadWriteLockType.Write, buffer.Lock);

            return temporary;
        }

        public Page ModifyPage(PageBufferEntry buffer)
        {
            if (!ModifidBuffers.TryGetValue(buffer.Position, out var cache))
            {
                buffer.Lock.EnterWriteLock();
                cache = new ModifyBufferCache(buffer, buffer.Page.CreateCopy());

                ModifidBuffers[buffer.Position] = cache;
                PushLockWaitForRelease(ReadWriteLockType.Write, buffer.Lock);
            }

            return cache.Temporary;
        }

        public Page GetModifiedTemporaryPage(PagePosition pos)
        {
            if (ModifidBuffers.TryGetValue(pos, out var cache))
            {
                return cache.Temporary;
            }

            return null;
        }

        public void PushLockWaitForRelease(ReadWriteLockType lockType, IReadWriteLock readWriteLock)
        {
            _txLocksWaitForRelease.Push((lockType, readWriteLock));
        }

        public void Reset()
        {
            _journal.Clear();
            _modifidBuffers.Clear();
            _txLocksWaitForRelease.Clear();
        }

        public void Commit()
        {
            CopyTemporaryToPages();
            ReleaseLockResources();
            Reset();
        }

        private void CopyTemporaryToPages()
        {
            foreach (var item in _modifidBuffers)
            {
                var page = item.Value.Buffer.Page;
                var temporary = item.Value.Temporary;

                temporary.CopyTo(page);
            }
        }

        private void ReleaseLockResources()
        {
            lock (Buffers.SyncRoot)
            {
                while (_txLocksWaitForRelease.Count != 0)
                {
                    var (lockType, lockEntry) = _txLocksWaitForRelease.Pop();

                    switch (lockType)
                    {
                        case ReadWriteLockType.Read:
                            lockEntry.ExitReadLock();
                            break;
                        case ReadWriteLockType.Write:
                            lockEntry.ExitWriteLock();
                            break;
                    }

                    if (lockEntry.Target is PageBufferEntry entry)
                    {
                        entry.Count--;
                    }
                }
            }
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        internal struct ModifyBufferCache
        {
            public Page Temporary;

            public PageBufferEntry Buffer;

            public ModifyBufferCache(PageBufferEntry buffer, Page temporary)
            {
                Buffer = buffer;
                Temporary = temporary;
            }
        }
    }
}
