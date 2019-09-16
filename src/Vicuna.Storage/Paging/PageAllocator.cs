namespace Vicuna.Engine.Paging
{
    public abstract class PageAllocator
    {
        public abstract void Alloc();

        public abstract void Free();
    }
}
