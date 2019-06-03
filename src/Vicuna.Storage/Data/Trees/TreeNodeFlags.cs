using System;

namespace Vicuna.Storage.Data.Trees
{
    [Flags]
    public enum TreeNodeFlags : byte
    {
        None = 0,

        Root = 1,

        Leaf = 3,

        Branch = 3
    }
}
