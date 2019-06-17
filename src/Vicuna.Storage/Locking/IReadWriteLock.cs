namespace Vicuna.Engine.Locking
{
    public interface IReadWriteLock
    {
        int ReadCount { get; }

        int WaitCount { get; }

        object Target { get; }

        bool IsReadLockHeld { get; }

        bool IsWriteLockHeld { get; }

        void ExitReadLock();

        void ExitWriteLock();

        void EnterReadLock();

        void EnterWriteLock();
    }
}
