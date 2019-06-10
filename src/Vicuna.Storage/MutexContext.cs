using System;
using System.Threading;

namespace Vicuna.Storage
{
    public struct MutexContext : IDisposable
    {
        private readonly LockMode _mode;

        private readonly ReaderWriterLockSlim _mutex;

        public static IDisposable Create(ReaderWriterLockSlim mutex, LockMode mode)
        {
            switch (mode)
            {
                case LockMode.S_LOCK:
                    mutex?.EnterReadLock();
                    return new MutexContext(mutex, mode);
                case LockMode.X_LOCK:
                    mutex?.EnterWriteLock();
                    return new MutexContext(mutex, mode);
                default:
                    return new MutexContext(mutex, mode);
            }
        }

        public MutexContext(ReaderWriterLockSlim mutex, LockMode flags)
        {
            _mode = flags;
            _mutex = mutex;
        }

        public void Dispose()
        {
            switch (_mode)
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
