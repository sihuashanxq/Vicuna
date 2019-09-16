using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Data.Extensions
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

        public static Span<byte> GetString(this Span<byte> @this, int index, out int size)
        {
            if (index < 0 || index > @this.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            switch ((DataSize)(@this[index] & DataTypeConstants.DataSizeMask))
            {
                case DataSize.Size_1:
                    size = @this[index + sizeof(byte)];
                    return @this.Slice(index + sizeof(byte), size);
                case DataSize.Size_2:
                    size = BitConverter.ToUInt16(@this.Slice(index, sizeof(ushort)));
                    return @this.Slice(index + sizeof(ushort), size);
                case DataSize.Size_4:
                    size = BitConverter.ToInt32(@this.Slice(index, sizeof(int)));
                    return @this.Slice(index + sizeof(int), size);
                default:
                    throw new InvalidOperationException();
            }
        }
    }
}
