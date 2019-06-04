using System;
using System.Threading;

namespace Vicuna.Storage.Buffers
{
    public struct MutexContext : IDisposable
    {
        private readonly LockMode _flags;

        private readonly ReaderWriterLockSlim _mutex;

        public static IDisposable Create(ReaderWriterLockSlim mutex, LockMode flags)
        {
            switch (flags)
            {
                case LockMode.S_LOCK:
                    mutex?.EnterReadLock();
                    return new MutexContext(mutex, flags);
                case LockMode.X_LOCK:
                    mutex?.EnterWriteLock();
                    return new MutexContext(mutex, flags);
                default:
                    return new MutexContext(mutex, flags);
            }
        }

        public MutexContext(ReaderWriterLockSlim mutex, LockMode flags)
        {
            _flags = flags;
            _mutex = mutex;
        }

        public void Dispose()
        {
            switch (_flags)
            {
                case LockMode.S_LOCK:
                    _mutex?.ExitReadLock();
                    break;
                case LockMode.X_LOCK:
                    _mutex?.ExitWriteLock();
                    break;
            }
        }
    }
}
