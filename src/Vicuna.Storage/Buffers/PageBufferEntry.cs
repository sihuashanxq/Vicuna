using Vicuna.Engine.Paging;
using Vicuna.Engine.Locking;

namespace Vicuna.Engine.Buffers
{
    public class PageBufferEntry
    {
        public Page Page;

        public long Count;

        public long OldLSN;

        public long NewLSN;

        public PageBufferEntry Prev;

        public PageBufferEntry Next;

        public PageBufferState State;

        public PagePosition Position;

        public LatchEntry Latch { get; }

        public readonly ReadWriteLock Lock;

        public PageBufferEntry(PageBufferState state)
        {
            State = state;
            Latch = new LatchEntry(this);
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
