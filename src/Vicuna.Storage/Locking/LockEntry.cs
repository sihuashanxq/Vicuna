using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Locking
{
    public class LockEntry
    {
        public int Thread;

        public byte[] Bits;

        public object Index;

        public LockFlags Flags;

        public PagePosition Page;

        public LockEntry WaitLock;

        public object Transaction;

        public bool IsTable => Flags.HasFlag(LockFlags.Table);

        public bool IsWaiting => Flags.HasFlag(LockFlags.Waiting);

        public bool IsExclusive => Flags.HasFlag(LockFlags.Exclusive);
    }
}
