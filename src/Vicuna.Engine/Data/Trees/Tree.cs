using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Data.Tables;
using Vicuna.Engine.Data.Trees.Fixed;
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
        private readonly Index _index;

        private readonly TreeRootHeader _root;

        private readonly PageAllocator _pageAllocator;

        public TreeRootHeader Root => _root;

        public const ushort MaxEntrySizeInPage = (Constants.PageSize - Constants.PageHeaderSize - Constants.PageFooterSize) / 2 - TreeNodeHeader.SizeOf - TreeNodeVersionHeader.SizeOf;

        public Tree(Index index, TreeRootHeader root, PageAllocator pageAllocator)
        {
            _root = root;
            _index = index;
            _pageAllocator = pageAllocator;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsLeaf(BufferEntry buffer)
        {
            return buffer.Page.Header.Cast<TreePageHeader>().NodeFlags.HasFlag(TreeNodeFlags.Leaf);
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

            return EngineEnviorment.LockManager.Lock(ref req);
        }

        private BufferEntry GetBufferForKey(LowLevelTransaction lltx, Span<byte> key, byte depth)
        {
            var buffer = lltx.Buffers.GetEntry(_root.FileId, _root.PageNumber);

            using (var tx = lltx.StartNew())
            {
                while (true)
                {
                    if (IsLeaf(buffer))
                    {
                        break;
                    }

                    var page = lltx.HasBufferLatch(buffer, LatchFlags.Read) ? buffer.Page.AsTree() : tx.GetPage(buffer).AsTree();
                    if (page.Depth == depth)
                    {
                        break;
                    }

                    buffer = tx.Buffers.GetEntry(page.FindPage(key));
                }
            }

            return buffer;
        }

        private TreePage GetCursorForUpdate(LowLevelTransaction ttx, Span<byte> key, byte depth)
        {
            var buffer = GetBufferForKey(ttx, key, depth);
            if (buffer == null)
            {
                throw new NullReferenceException($"can't find a page for the key:{key.ToString()}");
            }

            var page = ttx.ModifyPage(buffer);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            return page.AsTree();
        }

        private TreePage GetCursorForQuery(LowLevelTransaction ttx, Span<byte> key, byte depth, TreeNodeQueryMode mode = TreeNodeQueryMode.Lte)
        {
            var buffer = GetBufferForKey(ttx, key, depth);
            if (buffer == null)
            {
                throw new NullReferenceException($"can't find a page for the key:{key.ToString()}");
            }

            var page = ttx.GetPage(buffer);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            return page.AsTree(mode);
        }
    }
}
