using System.Collections.Generic;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine
{
    public class EngineEnviorment
    {
        static EngineEnviorment()
        {
            LockManager = new LockManager();
            PageManager = new PageManager(null);
            TransManager = new TransactionManager();
            Transactions = new Dictionary<long, Transaction>();
        }

        public static PageManager PageManager { get; }

        public static LockManager LockManager { get; }

        public static TransactionManager TransManager { get; }

        public static Dictionary<long, Transaction> Transactions { get; }
    }

    public class TransactionManager
    {

    }
}
