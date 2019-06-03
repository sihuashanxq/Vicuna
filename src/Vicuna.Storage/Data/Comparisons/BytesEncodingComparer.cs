using System;
using System.Runtime.CompilerServices;
using Vicuna.Storage.Data.Extensions;

namespace Vicuna.Storage.Data
{
    public unsafe class BytesEncodingComparer
    {
        public static int CompareTo(Span<byte> s1, Span<byte> s2, StringCompareMode model = StringCompareMode.None)
        {
            var i = 0;
            var n = 0;

            for (; i < s1.Length && n < s2.Length;)
            {
                var v = 0;
                var t1 = (DataType)(s1[i++] & 0xC0);
                var t2 = (DataType)(s2[n++] & 0xC0);
                if (t1 != t2 && (!t1.IsPrimitive() || !t2.IsPrimitive()))
                {
                    return t1 - t2;
                }

                switch (t1)
                {
                    case DataType.Char:
                        v = CompareTo(s1.ToChar(i), s2, ref n, t2);
                        i += sizeof(char);
                        break;
                    case DataType.Byte:
                        v = CompareTo(s1[i], s2, ref n, t2);
                        i += sizeof(byte);
                        break;
                    case DataType.Int16:
                        v = CompareTo(s1.ToInt16(i), s2, ref n, t2);
                        i += sizeof(short);
                        break;
                    case DataType.Int32:
                        v = CompareTo(s1.ToInt32(i), s2, ref n, t2);
                        i += sizeof(int);
                        break;
                    case DataType.Int64:
                        v = CompareTo(s1.ToInt64(i), s2, ref n, t2);
                        i += sizeof(long);
                        break;
                    case DataType.UInt16:
                        v = CompareTo(s1.ToUInt16(i), s2, ref n, t2);
                        i += sizeof(ushort);
                        break;
                    case DataType.UInt32:
                        v = CompareTo(s1.ToUInt32(i), s2, ref n, t2);
                        i += sizeof(uint);
                        break;
                    case DataType.UInt64:
                        v = CompareTo(s1.ToUInt64(i), s2, ref n, t2);
                        i += sizeof(ulong);
                        break;
                    case DataType.Single:
                        v = CompareTo(s1.ToSingle(i), s2, ref n, t2);
                        i += sizeof(float);
                        break;
                    case DataType.Double:
                        v = CompareTo(s1.ToDouble(i), s2, ref n, t2);
                        i += sizeof(double);
                        break;
                    case DataType.Boolean:
                        v = CompareTo(s1.ToBoolean(i), s2, ref n, t2);
                        i += sizeof(bool);
                        break;
                    case DataType.DateTime:
                        v = s1.ToInt64(i).CompareTo(s2.ToInt64(n));
                        i += sizeof(long);
                        n += sizeof(long);
                        break;
                    case DataType.String:
                        break;
                }

                if (v != 0)
                {
                    return v;
                }
            }

            return s1.Length - s2.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareTo(byte* p1, byte* p2, int len1, int len2)
        {
            var lp1 = p1;
            var lp2 = p2;
            var len = Math.Min(len1, len2);

            for (var i = 0; i < len / 8; i++)
            {
                if (*(long*)lp1 != *(long*)lp2)
                {
                    if (*(int*)lp1 == *(int*)lp2)
                    {
                        lp1 += 4;
                        lp2 += 4;
                    }

                    return CompareTo(lp1, lp2, sizeof(int));
                }

                lp1 += 8;
                lp2 += 8;
            }

            if ((len & 0x04) != 0)
            {
                if (*(int*)lp1 != *(int*)lp2)
                {
                    return CompareTo(lp1, lp2, sizeof(int));
                }

                lp1 += 4;
                lp2 += 4;
            }

            if ((len & 0x02) != 0)
            {
                if (*(short*)lp1 != *(short*)lp2)
                {
                    return CompareTo(lp1, lp2, sizeof(short));
                }

                lp1 += 2;
                lp2 += 2;
            }

            if ((len & 0x01) != 0)
            {
                var flag = *lp1 - *lp2;
                if (flag != 0)
                {
                    return flag;
                }
            }

            return len1 - len2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareTo(byte* p1, byte* p2, int len)
        {
            for (var n = 0; n < len; n++)
            {
                var v = p1[n] - p2[n];
                if (v == 0)
                {
                    continue;
                }

                return v;
            }

            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareTo(IComparable comparable, Span<byte> dest, ref int index, DataType type)
        {
            var i = index;

            switch (type)
            {
                case DataType.Char:
                    index += sizeof(char);
                    return comparable.CompareTo(dest.ToChar(i));
                case DataType.Byte:
                    index += sizeof(byte);
                    return comparable.CompareTo((char)dest[i]);
                case DataType.Int16:
                    index += sizeof(short);
                    return comparable.CompareTo(dest.ToInt16(i));
                case DataType.Int32:
                    index += sizeof(int);
                    return comparable.CompareTo(dest.ToInt32(i));
                case DataType.Int64:
                    index += sizeof(long);
                    return comparable.CompareTo(dest.ToInt64(i));
                case DataType.UInt16:
                    index += sizeof(ushort);
                    return comparable.CompareTo(dest.ToUInt16(i));
                case DataType.UInt32:
                    index += sizeof(uint);
                    return comparable.CompareTo(dest.ToUInt32(i));
                case DataType.UInt64:
                    index += sizeof(ulong);
                    return comparable.CompareTo(dest.ToUInt64(i));
                case DataType.Single:
                    index += sizeof(float);
                    return comparable.CompareTo(dest.ToSingle(i));
                case DataType.Double:
                    index += sizeof(double);
                    return comparable.CompareTo(dest.ToDouble(i));
                case DataType.Boolean:
                    index += sizeof(bool);
                    return comparable.CompareTo(dest.ToBoolean(i));
                default:
                    throw new InvalidOperationException($"data type:{type} is not a primitive type!");
            }
        }
    }
}
