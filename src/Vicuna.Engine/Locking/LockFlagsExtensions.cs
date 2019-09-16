using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Locking
{
    public static class LockFlagsExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsTable(this LockFlags flags)
        {
            return flags.HasFlag(LockFlags.Table);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsDocument(this LockFlags flags)
        {
            return flags.HasFlag(LockFlags.Document);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsExclusive(this LockFlags flags)
        {
            return flags.HasFlag(LockFlags.Exclusive);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsWaitting(this LockFlags flags)
        {
            return flags.HasFlag(LockFlags.Waiting);
        }
    }
}
