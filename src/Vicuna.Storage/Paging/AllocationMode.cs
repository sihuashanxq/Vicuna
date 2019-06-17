namespace Vicuna.Engine.Paging
{
    public enum AllocationMode
    {
        /// <summary>
        /// 常规方式(先从空闲页分配,然后从文件尾部开始分配)
        /// </summary>
        Normal,

        /// <summary>
        /// 从文件尾部开始分配
        /// </summary>
        Tail
    }
}
