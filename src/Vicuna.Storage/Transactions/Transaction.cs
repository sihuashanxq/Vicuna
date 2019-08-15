using System.Collections.Generic;
using System.Threading;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Locking;

namespace Vicuna.Engine.Transactions
{
    public class Transaction
    {
        public long Id { get; set; }

        public byte DeadFlags { get; set; }

        public LockEntry WaitLock { get; set; }

        public TransactionState State { get; set; }

        public LinkedList<LockEntry> RecLocks { get; }

        public ManualResetEventSlim WaitEvent { get; }

        public Transaction()
        {
            RecLocks = new LinkedList<LockEntry>();
            WaitEvent = new ManualResetEventSlim(false);
        }

        public void Commit()
        {

        }

        public void Rollback()
        {

        }
    }

    public enum TransactionState
    {
        Running,

        Waitting,

        Rollback,

        Commit
    }
}
