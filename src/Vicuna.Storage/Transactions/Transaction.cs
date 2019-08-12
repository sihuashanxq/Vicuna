using System.Collections.Generic;
using System.Threading;
using Vicuna.Engine.Locking;

namespace Vicuna.Engine.Transactions
{
    public class Transaction
    {
        public LockEntry WaitLock { get; set; }

        public LinkedList<LockEntry> Locks { get; }

        public ManualResetEventSlim WaitEvent { get; }

        public Transaction()
        {
            WaitEvent = new ManualResetEventSlim(true);
        }

        public long Id { get; }

        public void Commit()
        {

        }

        public void Rollback()
        {

        }
    }
}
