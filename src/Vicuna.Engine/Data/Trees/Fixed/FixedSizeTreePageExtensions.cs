using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public static class FixedSizeTreePageExtensions
    {
        public static FixedSizeTreePage AsFixed(this Page page)
        {
            if (page == null)
            {
                return null;
            }

            return new FixedSizeTreePage(page.Data);
        }
    }
}
