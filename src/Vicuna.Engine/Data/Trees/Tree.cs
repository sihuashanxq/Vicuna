using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees
{
    public enum TableIndexType
    {
        Cluster,

        Unique,

        Universale
    }

    public struct TableIndexSchema
    {
        public Tree Tree;

        public TableIndexType Type;

        public bool IsUnique => Type != TableIndexType.Universale;

        public bool IsCluster => Type == TableIndexType.Cluster;
    }

    public partial class Tree
    {
        public Index Index { get; }

        public PagePosition Root { get; }

        public const ushort MaxEntrySizeInPage = (Constants.PageSize - Constants.PageHeaderSize - Constants.PageFooterSize) / 2 - TreeNodeHeader.SizeOf - TreeNodeTransactionHeader.SizeOf;

        public Tree(PagePosition root)
        {
            Root = root;
        }
    }
}
