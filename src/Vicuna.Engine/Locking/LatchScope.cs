using System;
using System.Threading;

namespace Vicuna.Engine.Locking
{
    public class LatchScope : IDisposable
    {
        const int Released = 1;

        const int UnReleased = 0;

        private int _release = UnReleased;

        public LatchEntry Latch { get; }

        public LatchFlags Flags { get; private set; }

        public LatchScope(LatchEntry latch, LatchFlags flags)
        {
            Flags = flags;
            Latch = latch ?? throw new ArgumentNullException(nameof(latch));
        }

        /// <summary>
        /// thread unsafe
        /// </summary>
        public bool UpgrateWrite()
        {
            if (_release == UnReleased)
            {
                switch (Flags)
                {
                    case LatchFlags.Write:
                    case LatchFlags.RWWrite:
                        return true;
                    case LatchFlags.RWRead:
                        //don't wait
                        var ok = Latch._internalLock.TryEnterWriteLock(0);
                        Flags = ok ? LatchFlags.RWWrite : Flags;
                        return ok;
                    default:
                        throw new InvalidOperationException();
                }
            }

            return false;
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _release, Released, UnReleased) == UnReleased)
            {
                switch (Flags)
                {
                    case LatchFlags.Read:
                        Latch.ExitReadScope();
                        break;
                    case LatchFlags.Write:
                        Latch.ExitWriteScope();
                        break;
                    case LatchFlags.RWRead:
                        Latch.ExitReadWriteScope();
                        break;
                    case LatchFlags.RWWrite:
                        Latch.ExitWriteScope();
                        Latch.ExitReadWriteScope();
                        break;
                }
            }
        }
    }
}
