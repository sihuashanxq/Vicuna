namespace Vicuna.Engine.Locking
{
    public enum LatchFlags : byte
    {
        Read = 0,

        Write = 1,

        RWRead = 2,

        RWWrite = 3
    }
}
