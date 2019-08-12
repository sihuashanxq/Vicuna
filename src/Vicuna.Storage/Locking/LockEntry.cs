using System;
using System.Collections.Generic;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Locking
{
    public class LockEntry
    {
        public int Thread;

        public byte[] Bits;

        public Index Index;

        public LockFlags Flags;

        public PagePosition Page;

        public LockEntry WaitEntry;

        public Transaction Transaction;

        public LinkedListNode<LockEntry> TNode;

        public LinkedListNode<LockEntry> GNode;

        public int Count => IsTable ? 1 : Bits.Length * sizeof(byte);

        public bool IsTable => Flags.HasFlag(LockFlags.Table);

        public bool IsWaiting => Flags.HasFlag(LockFlags.Waiting);

        public bool IsExclusive => Flags.HasFlag(LockFlags.Exclusive);

        public LockEntry(PagePosition page, LockFlags flags, int recordCount)
        {
            Page = page;
            Flags = flags;
            Bits = flags.HasFlag(LockFlags.Table) ? new byte[0] : new byte[recordCount / 8 + 8];
        }

        public byte GetBit(int index)
        {
            return (byte)(Bits[index / 8] >> index % 8 & 1);
        }

        public void SetBit(int index, byte bit)
        {
            Bits[index / 8] |= (byte)(bit << index % 8);
        }

        public int GetMarkedIndex()
        {
            for (var i = 0; i < Bits.Length; i++)
            {
                if (Bits[i] == 0)
                {
                    continue;
                }

                for (var n = 0; n < sizeof(byte); n++)
                {
                    if ((Bits[i] & (1 << n)) != 0)
                    {
                        return i * sizeof(byte) + n;
                    }
                }
            }

            return -1;
        }

        public void MoveBits(int index)
        {
            const byte Bit8Mask = 0x80;
            const byte Bit7Mask = 0x7F;

            var mid = index / 8;
            var bit = Bits[mid] & Bit8Mask;

            for (var i = mid + 1; i < Bits.Length; i++)
            {
                var top = Bits[i] & Bit8Mask;

                Bits[i] = (byte)(bit | (Bits[i] & Bit7Mask));

                bit = top;
            }

            Bits[mid] = (byte)(Bits[mid] & (byte.MaxValue << (index % 8 + 1)) | Bits[mid] & (byte.MaxValue >> (8 - index % 8)));
        }

        public void ExtendCapacity(int capacity)
        {
            var bits = new byte[Bits.Length + (capacity % 8 == 0 ? capacity / 8 : capacity / 8 + 1)];

            Array.Copy(Bits, bits, Bits.Length);

            Bits = bits;
        }
    }
}
