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
}