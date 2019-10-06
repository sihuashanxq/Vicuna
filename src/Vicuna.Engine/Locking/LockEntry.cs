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

        public Vicuna.Engine.Data.Tables.TableIndex Index;

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
        public void MoveBits(int index)
        {
            const byte Bit8Mask = 0x80;

            var mid = index >> 3;
            var mod = index % 8;
            var bit = (Bits[mid] & Bit8Mask) >> 7;

            for (var i = mid + 1; i < Bits.Length; i++)
            {
                var top = (Bits[i] & Bit8Mask) >> 7;

                Bits[i] = (byte)(bit | (Bits[i] << 1));

                bit = top;
            }

            Bits[mid] = (byte)(((Bits[mid] & byte.MaxValue << mod) << 1) | (Bits[mid] & (byte.MaxValue >> (8 - mod))));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetBits(int index)
        {
            var n = index / 8;
            if (n >= Bits.Length)
            {
                return;
            }

            var m = index % 8;
            if (m == 0)
            {
                Array.Clear(Bits, n, Bits.Length - n);
                return;
            }

            if (n < Bits.Length - 1)
            {
                Array.Clear(Bits, n + 1, Bits.Length - n - 1);
            }

            Bits[n] = (byte)(Bits[n] & (byte.MaxValue >> (8 - m)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyBitsTo(int index, byte[] bits)
        {
            var mid = index / 8;

            Array.Copy(Bits, mid, bits, 0, Count - mid);

            bits[0] = (byte)(bits[0] & (byte.MaxValue << (index % 8)));
            Bits[mid] = (byte)(Bits[mid] & (byte.MaxValue >> (8 - index % 8)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void CopyBitsTo(int index, byte* bits)
        {
            var mid = index / 8;

            Unsafe.CopyBlock(ref Bits[mid], ref *bits, (uint)(Count - mid));

            bits[0] = (byte)(bits[0] & (byte.MaxValue << (index % 8)));
            Bits[mid] = (byte)(Bits[mid] & (byte.MaxValue >> (8 - index % 8)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExtendCapacity(int cap, LockExtendDirection direction)
        {
            switch (direction)
            {
                case LockExtendDirection.Head:
                    ExtendHeadCapacity(cap);
                    break;
                default:
                    ExtendTailCapacity(cap);
                    break;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ExtendTailCapacity(int cap)
        {
            var len = cap % 8 == 0 ? cap >> 3 : (cap >> 3) + 1;
            var bits = new byte[Bits.Length + len];

            Array.Copy(Bits, bits, Bits.Length);

            Bits = bits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ExtendHeadCapacity(int cap)
        {
            var mod = cap % 8;
            var len = mod == 0 ? cap >> 3 : (cap >> 3) + 1;
            var bits = new byte[Bits.Length + len];

            if (mod == 0)
            {
                Array.Copy(Bits, 0, bits, len, Bits.Length);
                Bits = bits;
                return;
            }

            if (len > 1)
            {
                Array.Copy(Bits, 0, bits, len - 1, Bits.Length);
            }

            Bits = bits;

            for (var i = 0; i < mod; i++)
            {
                MoveBits((len - 1) << 3);
            }
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

    public enum LockExtendDirection
    {
        Head,

        Tail
    }
}