using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Locking
{
    [Flags]
    public enum LockFlags : byte
    {
        None = 0,

        Share = 1,

        Exclusive = 2,

        Table = 16,

        Document = 32,

        Waiting = 128
    }

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
