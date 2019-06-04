namespace Vicuna.Storage.Buffers
{
    public enum PageBufferEntryState
    {
        Clean,

        Dirty,

        Removed
    }

    public enum PageBufferEntryIOState
    {
        None,

        Reading,

        Writing,

        WaitForReading,

        WaitForWriting
    }
}
