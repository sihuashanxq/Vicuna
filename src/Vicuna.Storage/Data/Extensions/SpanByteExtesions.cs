using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Storage.Data.Extensions
{
    public static class SpanByteExtesions
    {
        public static char ToChar(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(char) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, char>(ref @this[index]);
        }

        public static short ToInt16(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(short) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, short>(ref @this[index]);
        }

        public static int ToInt32(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(int) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, int>(ref @this[index]);
        }

        public static long ToInt64(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(long) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, long>(ref @this[index]);
        }

        public static bool ToBoolean(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(bool) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, bool>(ref @this[index]);
        }

        public static ushort ToUInt16(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(ushort) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, ushort>(ref @this[index]);
        }

        public static uint ToUInt32(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(uint) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, uint>(ref @this[index]);
        }

        public static ulong ToUInt64(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(ulong) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, ulong>(ref @this[index]);
        }

        public static float ToSingle(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(float) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, float>(ref @this[index]);
        }

        public static double ToDouble(this Span<byte> @this, int index)
        {
            if (index < 0 || index + sizeof(double) > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return Unsafe.As<byte, double>(ref @this[index]);
        }
    }
}
