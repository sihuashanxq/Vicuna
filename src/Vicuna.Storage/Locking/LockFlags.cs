using System;
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
}
