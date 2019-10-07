using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees
{
    public static class TreeExtensions
    {
        public static TreePage AsTree(this Page page, TreeNodeEnumMode mode = TreeNodeEnumMode.Lte)
        {
            if (page == null)
            {
                return null;
            }

            return new TreePage(page.Data, mode);
        }

        public static bool HasValue(this TreeNodeHeaderFlags flags)
        {
            return flags.HasFlag(TreeNodeHeaderFlags.Data) && !flags.HasFlag(TreeNodeHeaderFlags.Overflow);
        }

        public static bool HasVersion(this TreeNodeHeaderFlags flags)
        {
            return flags.HasFlag(TreeNodeHeaderFlags.Primary) && flags.HasFlag(TreeNodeHeaderFlags.Data);
        }
    }
}
