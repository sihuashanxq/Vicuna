
namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FreeFixedTree
    {
        public FreeFixedTreeRootHeader Root { get; }

        public FreeFixedTree(FreeFixedTreeRootHeader root)
        {
            Root = root;
        }
    }
}
