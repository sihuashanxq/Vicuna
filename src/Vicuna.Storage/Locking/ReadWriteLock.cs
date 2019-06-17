using System.Runtime.CompilerServices;
using System.Threading;

namespace Vicuna.Engine.Locking
{
    public class ReadWriteLock : IReadWriteLock
    {
        private readonly object _target;

        private readonly ReaderWriterLockSlim _internalLock;

        public ReadWriteLock(object target, LockRecursionPolicy policy = LockRecursionPolicy.NoRecursion)
        {
            _target = target;
            _internalLock = new ReaderWriterLockSlim(policy);
        }

        public object Target
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _target;
        }

        public int ReadCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _internalLock.CurrentReadCount;
        }

        public int WaitCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _internalLock.WaitingReadCount + _internalLock.WaitingUpgradeCount + _internalLock.WaitingWriteCount;
        }

        public bool IsReadLockHeld
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _internalLock.IsReadLockHeld;
        }

        public bool IsWriteLockHeld
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _internalLock.IsWriteLockHeld;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnterReadLock()
        {
            _internalLock.EnterReadLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnterWriteLock()
        {
            _internalLock.EnterWriteLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExitReadLock()
        {
            _internalLock.ExitReadLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExitWriteLock()
        {
            _internalLock.ExitWriteLock();
        }
    }
}
