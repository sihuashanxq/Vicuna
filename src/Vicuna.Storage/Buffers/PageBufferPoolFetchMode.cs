using System;

namespace Vicuna.Storage.Buffers
{
    [Flags]
    public enum PageBufferPoolFetchMode : byte
    {
        Normal = 0,

        NoWait = 1,

        NoFlushLRU = 2,

        JustBufferPool = 4
    }
}
