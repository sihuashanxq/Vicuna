using System;

namespace Vicuna.Engine.Buffers
{
    [Flags]
    public enum PageBufferPeekFlags : byte
    {
        None = 0,

        NoneMoveLRU = 1,

        NoneWaitReading = 2
    }
}
