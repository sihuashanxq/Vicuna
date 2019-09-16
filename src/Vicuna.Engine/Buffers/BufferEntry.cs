using Vicuna.Engine.Paging;
using Vicuna.Engine.Locking;

namespace Vicuna.Engine.Buffers
{
    public class BufferEntry
    {
        public Page Page;

        public long Count;

        public long OldLSN;

        public long NewLSN;

        public BufferEntry Prev;

        public BufferEntry Next;

        public BufferState State;

        public PagePosition Position;

        public readonly LatchEntry Latch;

        public BufferEntry(BufferState state)
        {
            State = state;
            Latch = new LatchEntry(this);
        }

        public BufferEntry(BufferState state, PagePosition pos)
        {
            State = state;
            Latch = new LatchEntry(this);
            Position = pos;
        }

        public override int GetHashCode()
        {
            return Position.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return Position.Equals(obj);
        }
    }
}
