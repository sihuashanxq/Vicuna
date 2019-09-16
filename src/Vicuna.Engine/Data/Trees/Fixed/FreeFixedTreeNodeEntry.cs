using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public ref struct FreeFixedTreeNodeEntry
    {
        public short Index { get; set; }

        public bool IsBranch { get; set; }

        public byte DataSize { get; set; }

        public Span<byte> Buffer { get; set; }

        public static FreeFixedTreeNodeEntry Empty => new FreeFixedTreeNodeEntry()
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
            get => Buffer.Slice(FreeFixedTreeNodeHeader.SizeOf, DataSize);
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

        public ref FreeFixedTreeNodeHeader Header
        {
            get => ref Unsafe.As<byte, FreeFixedTreeNodeHeader>(ref Buffer[0]);
        }
    }
}
