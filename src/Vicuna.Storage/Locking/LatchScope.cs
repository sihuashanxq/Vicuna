using System;
using System.Threading;

namespace Vicuna.Engine.Locking
{
    public class LatchScope : IDisposable
    {
        const int Released = 1;

        const int UnReleased = 0;

        private int _release = UnReleased;

        public LatchFlags Flags { get; }

        public LatchEntry Latch { get; }

        public LatchScope(LatchEntry latch, LatchFlags flags)
        {
            Flags = flags;
            Latch = latch ?? throw new ArgumentNullException(nameof(latch));
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _release, Released, UnReleased) == UnReleased)
            {
                switch (Flags)
                {
                    case LatchFlags.Read:
                        Latch.ExitReadScope();
                        break;
                    case LatchFlags.Write:
                        Latch.ExitWriteScope();
                        break;
                }
            } 
        }
    }
}
