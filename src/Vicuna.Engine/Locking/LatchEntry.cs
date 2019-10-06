using System.Runtime.CompilerServices;
using System.Threading;

namespace Vicuna.Engine.Locking
{
    public class LatchEntry
    {
        private readonly object _target;

        internal readonly ReaderWriterLockSlim _internalLock;

        public LatchEntry(object target, LockRecursionPolicy policy = LockRecursionPolicy.NoRecursion)
        {
            _target = target;
            _internalLock = new ReaderWriterLockSlim(policy);
        }

        public object Target => _target;

        /// <summary>
        /// debug only
        /// </summary>
        public int ReadCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _internalLock.CurrentReadCount;
        }

        /// <summary>
        /// debug only
        /// </summary>
        public int WaitCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _internalLock.WaitingReadCount + _internalLock.WaitingReadCount;
        }

        /// <summary>
        /// debug only
        /// </summary>
        public bool IsReadHeld
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _internalLock.IsReadLockHeld;
        }

        /// <summary>
        /// debug only
        /// </summary>
        public bool IsWriteHeld
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _internalLock.IsWriteLockHeld;
        }

        public void ExitReadScope()
        {
            _internalLock.ExitReadLock();
        }

        public void ExitWriteScope()
        {
            _internalLock.ExitWriteLock();
        }

        public void ExitReadWriteScope()
        {
            _internalLock.ExitUpgradeableReadLock();
        }

        public LatchScope EnterReadScope()
        {
            _internalLock.EnterReadLock();
            return new LatchScope(this, LatchFlags.Read);
        }

        public LatchScope EnterWriteScope()
        {
            _internalLock.EnterWriteLock();
            return new LatchScope(this, LatchFlags.Write);
        }

        public LatchScope EnterReadWriteScope()
        {
            _internalLock.EnterUpgradeableReadLock();
            return new LatchScope(this, LatchFlags.RWRead);
        }
    }
}
