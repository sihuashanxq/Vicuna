using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Vicuna.Engine.Data;
using Xunit;

namespace Vicuna.Storage.Test.Data.Comparisons
{
    public class BytesEncodingComparerTest
    {
        [Fact]
        public void CompareString()
        {
            var str1 = EncodeString("我们一起去吧,好a!");
            var str2 = EncodeString("我们一起去吧,好A!");

            Assert.False(BytesEncodingComparer.Compare(str1, str2) == 0);
            Assert.True(BytesEncodingComparer.Compare(str1, str2, StringCompareMode.IgnoreCase) == 0);
        }

        private byte[] EncodeString(string content)
        {
            var list = new List<byte>();
            var bytes = Encoding.UTF8.GetBytes(content);

            if (content.Length <= byte.MaxValue)
            {
                list.Add((byte)DataType.String | (byte)DataSize.Size_1);
                list.Add((byte)bytes.Length);
            }
            else if (content.Length <= ushort.MaxValue)
            {
                list.Add((byte)DataType.String | (byte)DataSize.Size_2);
                list.AddRange(BitConverter.GetBytes((ushort)bytes.Length));
            }
            else
            {
                list.Add((byte)DataType.String | (byte)DataSize.Size_4);
                list.AddRange(BitConverter.GetBytes(bytes.Length));
            }

            list.AddRange(bytes);
            return list.ToArray();
        }

        private byte[] EncodingNumeric(object number)
        {
            var list = new List<byte>();
            switch (number)
            {
                case char @char:
                    list.Add((byte)DataType.Char);
                    list.AddRange(BitConverter.GetBytes(@char));
                    break;
                case byte @byte:
                    list.Add((byte)DataType.Byte);
                    list.Add(@byte);
                    break;
                case short @short:
                    list.Add((byte)DataType.Int16);
                    list.AddRange(BitConverter.GetBytes(@short));
                    break;
                case int @int:
                    list.Add((byte)DataType.Int32);
                    list.AddRange(BitConverter.GetBytes(@int));
                    break;
                case long @long:
                    list.Add((byte)DataType.Int64);
                    list.AddRange(BitConverter.GetBytes(@long));
                    break;
                case ushort @ushort:
                    list.Add((byte)DataType.UInt16);
                    list.AddRange(BitConverter.GetBytes(@ushort));
                    break;
                case uint @uint:
                    list.Add((byte)DataType.UInt32);
                    list.AddRange(BitConverter.GetBytes(@uint));
                    break;
                case ulong @ulong:
                    list.Add((byte)DataType.UInt64);
                    list.AddRange(BitConverter.GetBytes(@ulong));
                    break;
                case float @float:
                    list.Add((byte)DataType.Single);
                    list.AddRange(BitConverter.GetBytes(@float));
                    break;
                case double @double:
                    list.Add((byte)DataType.Double);
                    list.AddRange(BitConverter.GetBytes(@double));
                    break;
                case bool @bool:
                    list.Add((byte)DataType.Boolean);
                    list.AddRange(BitConverter.GetBytes(@bool));
                    break;
            }

            return list.ToArray();
        }
    }
}
