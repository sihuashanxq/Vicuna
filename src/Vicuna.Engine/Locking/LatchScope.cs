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
                    case LatchFlags.Read:
                    case LatchFlags.Write:
                        throw new InvalidOperationException();
                    case LatchFlags.RWRead:
                        if (Latch._internalLock.TryEnterWriteLock(0))
                        {
                            Flags = LatchFlags.RWWrite;
                            return true;
                        }

                        return false;
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
