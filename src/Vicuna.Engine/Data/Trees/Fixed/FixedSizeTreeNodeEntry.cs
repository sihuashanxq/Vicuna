using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public ref struct FixedSizeTreeNodeEntry
    {
        public short Index { get; set; }

        public bool IsBranch { get; set; }

        public byte DataSize { get; set; }

        public Span<byte> Buffer { get; set; }

        public static FixedSizeTreeNodeEntry Empty => new FixedSizeTreeNodeEntry()
        {
            Index = -1,
            Buffer = Span<byte>.Empty,
        };

        public ref long Key
        {
            get => ref Header.PageNumber;
        }

        public Span<byte> Value
        {
            get => Buffer.Slice(FixedSizeTreeNodeHeader.SizeOf, DataSize);
        }

        public ref long PageNumber
        {
            get
            {
                if (!IsBranch)
                {
                    throw new InvalidOperationException("err api invoke!");
                }

                return ref Unsafe.As<byte, long>(ref Value[0]);
            }
        }

        public ref FixedSizeTreeNodeHeader Header
        {
            get => ref Unsafe.As<byte, FixedSizeTreeNodeHeader>(ref Buffer[0]);
        }
    }
}
