using System;

namespace Vicuna.Engine.Buffers
{
    [Flags]
    public enum BufferPeekFlags : byte
    {
        None = 0,

        KeepLRU = 1,

        NoneWait = 2
    }
}
