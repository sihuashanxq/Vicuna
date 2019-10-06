using System;
using System.Runtime.CompilerServices;

using Vicuna.Engine.Buffers;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

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
        private readonly TableIndex _index;

        private readonly TreeRootHeader _root;

        private readonly PageAllocator _pageAllocator;

        public TreeRootHeader Root => _root;

        public const ushort MaxKeySize = 512;

        public const ushort MaxBranchEntrySize = 768;

        public const ushort MaxEntrySizeInPage = (Constants.PageSize - Constants.PageHeaderSize - Constants.PageFooterSize) / 2 - TreeNodeHeader.SizeOf - TreeNodeVersionHeader.SizeOf;

        public Tree(TableIndex index, TreeRootHeader root, PageAllocator pageAllocator)
        {
            _root = root;
            _index = index;
            _pageAllocator = pageAllocator;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsBranch(BufferEntry buffer)
        {
            return buffer.Page.Header.Cast<TreePageHeader>().NodeFlags.HasFlag(TreeNodeFlags.Branch);
        }

        protected long BackUpUndoEntry(LowLevelTransaction lltx, TreePage page, int index)
        {
            return -1;
        }

        protected DBOperationFlags LockRec(LowLevelTransaction lltx, TreePage page, LockFlags flags)
        {
            var req = new LockRequest()
            {
                Flags = flags,
                Index = _index,
                Page = page.Position,
                Transaction = lltx.Transaction,
                RecordIndex = page.LastMatchIndex,
                RecordCount = page.TreeHeader.Count,
            };

            return DBOperationFlags.Ok;
            return EngineEnviorment.LockManager.Lock(ref req);
        }

        public bool TryGetEntry(LowLevelTransaction lltx, Span<byte> key, out long n)
        {
            var page = GetPageForQuery(lltx, key, Constants.BTreeLeafPageDepth);
            if (page == null)
            {
                n = 0;
                return false;
            }

            if (page.IsBranch)
            {
                n = 0;
                return false;
            }

            if (true)
            {
                page.Search(key);
            }

            if (page.LastMatch != 0 || page.LastMatchIndex < 0)
            {
                n = int.MinValue;
                return false;
            }

            n = BitConverter.ToInt32(page.GetNodeEntry(page.LastMatchIndex).Key.Slice(1));
            return true;
        }
    }
}
