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
            Ini();
        }

        public static void Ini()
        {
            LockManager = new LockManager();
            PageManager = new PageManager(new Dictionary<int, Storages.File>());
            TransManager = new TransactionManager();
            Transactions = new Dictionary<long, Transaction>()
            {
                [0] = Transaction
            };
        }

        public static Transaction Transaction = new Transaction { Id = 0 };

        public static PageManager PageManager { get; set; }

        public static LockManager LockManager { get; set; }

        public static TransactionManager TransManager { get; set; }

        public static Dictionary<long, Transaction> Transactions { get; set; }
    }

    public class TransactionManager
    {

    }
}
