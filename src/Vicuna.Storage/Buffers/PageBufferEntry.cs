using Vicuna.Storage.Paging;
using System.Threading;
using System;

namespace Vicuna.Storage.Buffers
{
    public class PageBufferEntry
    {
        public Page Page;

        public long OldLSN;

        public long NewLSN;

        public long LastTicks;

        public long ReferenceCount;

        public PageBufferEntry Prev;

        public PageBufferEntry Next;

        public PageBufferEntryState State;

        public PageBufferEntryIOState IOState;

        public readonly ReaderWriterLockSlim Mutex;

        public PageBufferEntry(Page page, PageBufferEntryState? state, PageBufferEntryIOState? ioState)
        {
            Page = page;
            State = state ?? PageBufferEntryState.Clean;
            IOState = ioState ?? PageBufferEntryIOState.None;
            LastTicks = DateTime.UtcNow.Ticks;
            Mutex = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        }

        public IDisposable EnterLock(LockMode mode)
        {
            return MutexContext.Create(Mutex, mode);
        }

        public override int GetHashCode()
        {
            return Page.Position.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return Page.Position.Equals(obj);
        }
    }
}
