using System;

namespace Vicuna.Engine.Data.Trees
{
    [Flags]
    public enum TreeNodeHeaderFlags : byte
    {
        Primary = 1,

        Data = 2,

        Page = 4,

        Overflow = 8
    }

    public static class TreeNodeHeaderFlagsExtensions
    {
        public static bool HasValue(this TreeNodeHeaderFlags flags)
        {
            return flags.HasFlag(TreeNodeHeaderFlags.Data) && !flags.HasFlag(TreeNodeHeaderFlags.Overflow);
        }

        public static bool HasVersion(this TreeNodeHeaderFlags flags)
        {
            return flags.HasFlag(TreeNodeHeaderFlags.Primary) && flags.HasFlag(TreeNodeHeaderFlags.Data);
        }
    }
}