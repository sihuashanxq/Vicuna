namespace Vicuna.Storage.Paging
{
    public interface IPageHeader
    {
        long GetLSN();

        long GetSizeOf();

        int GetStoreId();

        long GetPageNumber();

        PageHeaderFlags GetFlags();
    }
}
