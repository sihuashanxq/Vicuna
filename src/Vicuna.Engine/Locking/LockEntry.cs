using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Locking
{
    public class LockEntry
    {
        public int Thread;

        public byte[] Bits;

        public TableIndex Index;

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
            return (byte)(Bits[index >> 3] >> index % 8 & 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetBit(int index, byte bit)
        {
            Bits[index >> 3] |= (byte)(bit << index % 8);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetFirstBitSlot()
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
                        return (i << 3) + n;
                    }
                }
            }

            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MoveBitsUp(int index)
        {
            MoveBitsUp(index, Bits);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MoveBitsUp(int index, byte[] bits)
        {
            if (index >= Count)
            {
                return;
            }

            const byte Bit8Mask = 0x80;

            var mid = index >> 3;
            var mod = index % 8;
            var bit = (bits[mid] & Bit8Mask) >> 7;

            for (var i = mid + 1; i < bits.Length; i++)
            {
                var top = (bits[i] & Bit8Mask) >> 7;

                bits[i] = (byte)(bit | (bits[i] << 1));

                bit = top;
            }

            bits[mid] = (byte)(((bits[mid] & byte.MaxValue << mod) << 1) | (bits[mid] & (byte.MaxValue >> (8 - mod))));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MoveBitsUp(int index, byte[] bits, int bitCount)
        {
            for (var i = 0; i < bitCount; i++)
            {
                MoveBitsUp(index, bits);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MoveBitsDown()
        {
            MoveBitsDown(Bits);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MoveBitsDown(byte[] bits)
        {
            var bit = 0;

            for (var i = bits.Length - 1; i >= 0; i--)
            {
                var low = bits[i] & 1;

                bits[i] = (byte)((bit << 7) | (bits[i] >> 1));

                bit = low;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MoveBitsDown(byte[] bits, int bitCount)
        {
            for (var i = 0; i < bitCount; i++)
            {
                MoveBitsDown(bits);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void MoveBitsDown(byte* bits, int count)
        {
            var bit = 0;

            for (var i = count - 1; i >= 0; i--)
            {
                var low = bits[i] & 1;

                bits[i] = (byte)((bit << 7) | (bits[i] >> 1));

                bit = low;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void MoveBitsDown(byte* bits, int count, int bitCount)
        {
            for (var i = 0; i < bitCount; i++)
            {
                MoveBitsDown(bits, count);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void TuncateBits(int index)
        {
            if (index == 0)
            {
                Bits = new byte[0];
                return;
            }

            var n = index >> 3;
            var m = index % 8;
            var bits = new byte[Math.Min(n + 1, Bits.Length)];

            Array.Copy(Bits, bits, bits.Length);

            if (m == 0)
            {
                Array.Clear(bits, n, bits.Length - n);
                Bits = bits;
                return;
            }

            Array.Clear(bits, n + 1, bits.Length - n - 1);
            Bits = bits;
            Bits[n] = (byte)(Bits[n] & (byte.MaxValue >> (8 - m)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void CopyBitsTo(int index, byte* buffer)
        {
            var mid = index >> 3;
            var count = (uint)(Bits.Length - mid);
            var bits = Bits;

            Unsafe.CopyBlockUnaligned(ref *buffer, ref bits[mid], count);

            bits[mid] = (byte)(bits[mid] & (byte.MaxValue >> (8 - index % 8)));
            buffer[0] = (byte)(buffer[0] & (byte.MaxValue << (index % 8)));

            MoveBitsDown(buffer, (int)count, index % 8);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ExtendCapacity(int cap)
        {
            var len = cap % 8 == 0 ? cap >> 3 : (cap >> 3) + 1;
            var bits = new byte[Bits.Length + len];

            Array.Copy(Bits, bits, Bits.Length);

            Bits = bits;
        }

        public override string ToString()
        {
            var builder = new StringBuilder($" lock table :{Index }")
                .Append($" { (IsWaiting ? " in  wait state " : string.Empty)}")
                .Append($" by transaction {Transaction.Id} ");

            if (IsTable)
            {
                return builder.ToString();
            }

            builder.Append($" at page {Page}");
            builder.Append($" with records :");

            for (var i = 0; i < Bits.Length; i++)
            {
                builder.Append(new string(Convert.ToString(Bits[i], 2).PadLeft(8, '0').Reverse().ToArray())).Append("  ");
            }

            return builder.ToString();
        }
    }
}