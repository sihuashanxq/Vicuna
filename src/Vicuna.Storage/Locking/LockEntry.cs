using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Locking
{
    public class LockEntry
    {
        public int Thread;

        public byte[] Bits;

        public Index Index;

        public LockFlags Flags;

        public PagePosition Page;

        public Transaction Transaction;

        public LinkedListNode<LockEntry> TNode;

        public LinkedListNode<LockEntry> GNode;

        public int Count => IsTable ? 1 : Bits.Length * 8;

        public bool IsTable => Flags.HasFlag(LockFlags.Table);

        public bool IsWaiting => Flags.HasFlag(LockFlags.Waiting);

        public bool IsExclusive => Flags.HasFlag(LockFlags.Exclusive);

        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (IsTable)
                {
                    return false;
                }

                for (var i = 0; i < Bits.Length; i++)
                {
                    if (Bits[i] != 0)
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        public LockEntry()
        {

        }

        public LockEntry(PagePosition page, LockFlags flags, int recordCount)
        {
            Page = page;
            Flags = flags;
            Bits = flags.HasFlag(LockFlags.Table) ? new byte[0] : new byte[recordCount / 8 + 8];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte GetBit(int index)
        {
            return (byte) (Bits[index / 8] >> index % 8 & 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetBit(int index, byte bit)
        {
            Bits[index / 8] |= (byte) (bit << index % 8);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetFirstBitIndex()
        {
            for (var i = 0; i < Bits.Length; i++)
            {
                if (Bits[i] == 0)
                {
                    continue;
                }

                for (var n = 0; n < 8; n++)
                {
                    if ((Bits[i] & (1 << n)) != 0)
                    {
                        return i * 8 + n;
                    }
                }
            }

            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MoveBits(int index)
        {
            const byte Bit8Mask = 0x80;
            const byte Bit7Mask = 0x7F;

            var mid = index / 8;
            var bit = Bits[mid] & Bit8Mask;

            for (var i = mid + 1; i < Bits.Length; i++)
            {
                var top = Bits[i] & Bit8Mask;

                Bits[i] = (byte) (bit | (Bits[i] & Bit7Mask));

                bit = top;
            }

            Bits[mid] = (byte) (Bits[mid] & (byte.MaxValue << (index % 8 + 1)) | Bits[mid] & (byte.MaxValue >>(8 - index % 8)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetBits(int index)
        {
            var mid = index / 8;

            for (var i = mid + 1; i < Bits.Length; i++)
            {
                Bits[i] = 0;
            }

            Bits[mid] = (byte) (Bits[mid] & (byte.MaxValue >>(8 - index % 8)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyBitsTo(int index, byte[] bits)
        {
            var mid = index / 8;

            Array.Copy(Bits, mid, bits, 0, Count - mid);

            bits[0] = (byte) (bits[0] & (byte.MaxValue << (index % 8)));
            Bits[mid] = (byte) (Bits[mid] & (byte.MaxValue >>(8 - index % 8)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void CopyBitsTo(int index, byte * bits)
        {
            var mid = index / 8;

            Unsafe.CopyBlock(ref Bits[mid], ref * bits, (uint) (Count - mid));

            bits[0] = (byte) (bits[0] & (byte.MaxValue << (index % 8)));
            Bits[mid] = (byte) (Bits[mid] & (byte.MaxValue >>(8 - index % 8)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExtendCapacity(int capacity)
        {
            var bits = new byte[Bits.Length + (capacity % 8 == 0 ? capacity / 8 : capacity / 8 + 1)];

            Array.Copy(Bits, bits, Bits.Length);

            Bits = bits;
        }
    }
}