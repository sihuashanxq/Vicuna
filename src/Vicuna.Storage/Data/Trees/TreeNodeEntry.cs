using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Data.Trees
{
    public ref struct TreeNodeEntry
    {
        public ushort Slot;

        public short Index;

        public ushort Position;

        public Span<byte> Data;

        public static TreeNodeEntry Empty => new TreeNodeEntry()
        {
            Slot = 0,
            Index = -1,
            Data = Span<byte>.Empty,
            Position = 0
        };

        public Span<byte> Key
        {
            get
            {
                return Data.Slice(TreeNodeHeader.SizeOf, Header.KeySize);
            }
        }

        public Span<byte> Value
        {
            get
            {
                switch (Header.NodeFlags)
                {
                    case TreeNodeHeaderFlags.Data:
                        return Data.Slice(TreeNodeHeader.SizeOf + Header.KeySize + TreeNodeTransactionHeader.SizeOf, Header.DataSize);
                    case TreeNodeHeaderFlags.DataRefrence:
                        return Data.Slice(TreeNodeHeader.SizeOf + Header.KeySize, Header.DataSize);
                    default:
                        return Span<byte>.Empty;
                }
            }
        }

        public ref TreeNodeHeader Header
        {
            get => ref Unsafe.As<byte, TreeNodeHeader>(ref Data[0]);
        }

        public ref TreeNodeTransactionHeader Transaction
        {
            get
            {
                if (Header.NodeFlags != TreeNodeHeaderFlags.Data)
                {
                    throw new InvalidOperationException($"only data node has tx header!");
                }

                return ref Unsafe.As<byte, TreeNodeTransactionHeader>(ref Data[TreeNodeHeader.SizeOf + Header.KeySize]);
            }
        }
    }
}
