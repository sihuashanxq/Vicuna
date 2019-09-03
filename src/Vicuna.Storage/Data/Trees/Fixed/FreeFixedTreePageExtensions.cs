using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public static class FreeFixedTreePageExtensions
    {
        public static FreeFixedTreePage AsFixed(this Page page)
        {
            if (page == null)
            {
                return null;
            }

            return new FreeFixedTreePage(page.Data);
        }
    }
}
