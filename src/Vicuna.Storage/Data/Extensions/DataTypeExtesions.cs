namespace Vicuna.Storage.Data.Extensions
{
    public static class DataTypeExtesions
    {
        public static bool IsPrimitive(this DataType dataType)
        {
            switch (dataType)
            {
                case DataType.Char:
                case DataType.Byte:
                case DataType.Int16:
                case DataType.Int32:
                case DataType.Int64:
                case DataType.UInt16:
                case DataType.UInt32:
                case DataType.UInt64:
                case DataType.Single:
                case DataType.Double:
                case DataType.Boolean:
                default:
                    return false;
            }
        }
    }
}
