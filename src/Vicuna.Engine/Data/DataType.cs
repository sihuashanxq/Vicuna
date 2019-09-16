namespace Vicuna.Engine.Data
{
    /// <summary>
    /// low  6-bits is the value's type
    /// high 2-bits is the value size used byte count( string or object)
    /// such as "helloworld", the structure is a byte[] (0x0A0x0Ahelloworld)
    /// </summary>
    public enum DataType : byte
    {
        Null = 1,

        Char = 2,

        Byte = 3,

        Array = 4,

        Int16 = 5,

        Int32 = 6,

        Int64 = 7,

        UInt16 = 8,

        UInt32 = 9,

        UInt64 = 10,

        Single = 11,

        Double = 12,

        Object = 13,

        String = 14,

        Boolean = 15,

        DateTime = 16
    }
}
