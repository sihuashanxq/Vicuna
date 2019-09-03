using System;

namespace Vicuna.Engine.Data.Trees
{
    [Flags]
    public enum TreeNodeFlags : byte
    {
        None = 0,

        Root = 1,

        Leaf = 2,

        Branch = 4
    }
}
