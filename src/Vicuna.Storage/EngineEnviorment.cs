using System.Collections.Generic;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine
{
    public class EngineEnviorment
    {
        static EngineEnviorment()
        {
            LockManager = new LockManager();
            Transactions = new Dictionary<long, Transaction>();
        }

        public static LockManager LockManager { get; }

        public static Dictionary<long, Transaction> Transactions { get; }
    }
}
