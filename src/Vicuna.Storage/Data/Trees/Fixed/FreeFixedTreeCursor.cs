using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public struct FreeFixedTreeCursor
    {
        internal int Index;

        internal FreeFixedTree Tree;

        internal FreeFixedTreePage Current;

        public bool MoveNext(LowLevelTransaction lltx, out FreeFixedTreeNodeEntry entry)
        {
            ref var fixedHeader = ref Current.FixedHeader;
            if (fixedHeader.Count > Index)
            {
                entry = Current.GetNodeEntry(Index);
                Index++;
                return true;
            }

            if (fixedHeader.NextPageNumber <= 0)
            {
                entry = FreeFixedTreeNodeEntry.Empty;
                return false;
            }

            Index = 0;
            Current = lltx.GetPage(fixedHeader.FileId, fixedHeader.NextPageNumber).AsFixed();

            return MoveNext(lltx, out entry);
        }
    }
}
