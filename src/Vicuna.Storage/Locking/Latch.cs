using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Vicuna.Engine.Locking
{
    public class LatchEntry
    {
        private readonly object _target;

        private readonly ReaderWriterLockSlim _internalLock;

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

        public void ExitRead()
        {
#if (DEBUG)
            Debug.Assert(IsReadHeld);
#endif
            _internalLock.ExitReadLock();
        }

        public void ExitWrite()
        {
#if (DEBUG)
            Debug.Assert(IsWriteHeld);
#endif
            _internalLock.ExitWriteLock();
        }

        public LatchReleaserEntry EnterRead()
        {
            _internalLock.ExitReadLock();
#if (DEBUG)
            Debug.Assert(IsReadHeld);
#endif
            return new LatchReleaserEntry(this, LatchFlags.Read);
        }

        public LatchReleaserEntry EnterWrite()
        {
            _internalLock.ExitReadLock();
#if (DEBUG)
            Debug.Assert(IsWriteHeld);
#endif
            return new LatchReleaserEntry(this, LatchFlags.Write);
        }
    }
}
