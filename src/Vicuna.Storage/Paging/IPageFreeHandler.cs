namespace Vicuna.Engine.Paging
{
    public interface IPageFreeHandler
    {
        void Release(ReleaseContext ctx);

        int Allocate(AllocationContext ctx, PagePosition[] pos);
    }
}
