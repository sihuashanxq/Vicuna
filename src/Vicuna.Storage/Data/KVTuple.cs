using System;

namespace Vicuna.Engine.Data
{
    public ref struct KVTuple
    {
        public Span<byte> Key;

        public Span<byte> Value;

        public int Length => Key.Length + Value.Length;
    }
}
