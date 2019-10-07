using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees
{
    public static class TreeHelper
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort GetNodeSizeInPage(KVTuple kv, TreeNodeHeaderFlags nodeFlags)
        {
            var size = kv.Length > Tree.MaxEntrySizeInPage ? kv.Key.Length : kv.Length;
            size += TreeNodeHeader.SizeOf + sizeof(short);
            size += nodeFlags.HasVersion() ? TreeNodeVersionHeader.SizeOf : 0;
            return (ushort)size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static short ByteOffset<T1, T2>(ref T1 from, ref T2 to)
        {
            ref var bTo = ref Unsafe.As<T2, byte>(ref to);
            ref var bFrom = ref Unsafe.As<T1, byte>(ref from);

            var offset = Unsafe.ByteOffset(ref bFrom, ref bTo).ToInt32();
            if (offset > short.MaxValue)
            {
                throw new IndexOutOfRangeException($"{offset}");
            }

            return (short)offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsBranch(Page page)
        {
            return page.Header.Cast<TreePageHeader>().NodeFlags.HasFlag(TreeNodeFlags.Branch);
        }
    }
}
