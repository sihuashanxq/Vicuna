using Vicuna.Engine.Paging;
using Vicuna.Engine.Locking;

namespace Vicuna.Engine.Buffers
{
    public class PageBufferEntry
    {
        public Page Page;

        public long OldLSN;

        public long NewLSN;

        /// <summary>
        /// 引用计数
        /// </summary>
        public long Count;

        public PageBufferEntry Prev;

        public PageBufferEntry Next;

        public PageBufferState State;

        public PagePosition Position;

        public readonly ReadWriteLock Lock;

        public PageBufferEntry(PageBufferState state)
        {
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
