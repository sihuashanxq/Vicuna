namespace Vicuna.Storage.Paging
{
    public interface IPageTailer
    {
        long GetLSN();

        long GetSizeOf();
    }
}
