using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Data.Trees
{
    public ref struct TreeNodeEntry
    {
        public short Index;

        public Span<byte> Key;

        public Span<byte> Value;

        public Span<byte> Buffer;

        public Span<byte> Version;

        public static TreeNodeEntry Empty => new TreeNodeEntry()
        {
            Index = -1,
            Buffer = Span<byte>.Empty
        };

        public void SetKey(Span<byte> key)
        {
            key.CopyTo(Key);
        }

        public void SetValue(Span<byte> value)
        {
            value.CopyTo(Value);
        }

        public ref TreeNodeHeader Header
        {
            get => ref Unsafe.As<byte, TreeNodeHeader>(ref Buffer[0]);
        }

        public ref TreeNodeVersionHeader VersionHeader
        {
            get
            {
                if (!Header.NodeFlags.HasVersion())
                {
                    throw new InvalidOperationException($"err api invoke!");
                }

                return ref Unsafe.As<byte, TreeNodeVersionHeader>(ref Version[0]);
            }
        }
    }
}
