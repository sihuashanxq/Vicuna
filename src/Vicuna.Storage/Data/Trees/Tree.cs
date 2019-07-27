using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees
{
    public enum TableIndexType
    {
        Clustered,

        Unique,

        Universale
    }

    public struct TableIndexSchema
    {
        public Tree Tree;

        public TableIndexType Type;

        public bool IsUnique => Type != TableIndexType.Universale;

        public bool IsClustered => Type == TableIndexType.Clustered;
    }

    public partial class Tree
    {
        public PagePosition Root { get; }

        public TableIndexSchema Index { get; }

        public Tree(PagePosition root)
        {
            Root = root;
        }
    }
}
