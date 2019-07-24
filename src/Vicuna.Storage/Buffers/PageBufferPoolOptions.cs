namespace Vicuna.Engine.Buffers
{
    public class PageBufferPoolOptions
    {
        public long Limit { get; set; }

        public long LRULimit { get; set; }

        public uint AsyncFlushCount { get; set; } = 32;

        public uint SyncFlushMidpoint { get; set; } = 75;
    }
}
