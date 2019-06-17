using Vicuna.Engine.Paging;
using Vicuna.Engine.Locking;

namespace Vicuna.Engine.Buffers
{
    public class PageBufferEntry
    {
        public long Refs;

        public Page Page;

        public long OldLSN;

        public long NewLSN;

        public PageBufferEntry Prev;

        public PageBufferEntry Next;

        public PageBufferState State;

        public readonly ReadWriteLock Lock;

        public PageBufferEntry(Page page, PageBufferState state)
        {
            Page = page;
            State = state;
            Lock = new ReadWriteLock(this);
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
