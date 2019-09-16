using System;

namespace Vicuna.Engine.Buffers
{
    [Flags]
    public enum BufferSeekFlags : byte
    {
        None = 0,

        NoLRU = 1,

        NoWait = 2
    }
}
