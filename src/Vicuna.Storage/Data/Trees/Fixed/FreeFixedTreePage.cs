using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public class FreeFixedTreePage : Page
    {
        public int Level;

        public int LastMatch;

        public int LastMatchIndex;

        public FreeFixedTreePage(byte[] data, int level) : base(data)
        {
            Level = level;
            LastMatch = 0;
            LastMatchIndex = -1;
        }

        public bool IsLeaf
        {
            get => FixedHeader.NodeFlags.HasFlag(TreeNodeFlags.Leaf);
        }

        public bool IsRoot
        {
            get => FixedHeader.NodeFlags.HasFlag(TreeNodeFlags.Root);
        }

        public bool IsBranch
        {
            get => FixedHeader.NodeFlags.HasFlag(TreeNodeFlags.Branch);
        }

        public long FirstKey
        {
            get => GetNodeKey(0);
        }

        public long LastKey
        {
            get => GetNodeKey(FixedHeader.Count - (IsLeaf ? 1 : 2));
        }

        public ref FreeFixedTreePageHeader FixedHeader
        {
            get => ref Header.Cast<FreeFixedTreePageHeader>();
        }

        public bool Alloc(int index, out FreeFixedTreeNodeEntry entry)
        {
            ref var fixedHeader = ref FixedHeader;
            var ptr = GetNodePtr(index);
            var usedSize = GetUsedSize(ref fixedHeader);
            var nodeSize = GetNodeSize(ref fixedHeader);
            if (nodeSize + usedSize > Constants.PageSize)
            {
                entry = FreeFixedTreeNodeEntry.Empty;
                return false;
            }

            if (index <= fixedHeader.Count - 1)
            {
                var len = (fixedHeader.Count - index) * nodeSize;
                var to = ReadAt(ptr + nodeSize, len);
                var from = ReadAt(ptr, len);

                from.CopyTo(to);
            }

            entry = new FreeFixedTreeNodeEntry()
            {
                Index = (short)index,
                Buffer = ReadAt(ptr, nodeSize),
                DataSize = fixedHeader.DataElementSize,
                IsBranch = fixedHeader.NodeFlags.HasFlag(TreeNodeFlags.Branch)
            };

            fixedHeader.Count++;
            return true;
        }

        public FreeFixedTreeNodeEntry Remove(int index)
        {
            ref var fixedHeader = ref FixedHeader;
            if (index < 0 || index > fixedHeader.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var size = GetNodeSize(ref fixedHeader);
            var last = GetNodePtr(fixedHeader.Count - 1);

            if (index != fixedHeader.Count - 1)
            {
                var ptr = GetNodePtr(index);
                var len = size * (fixedHeader.Count - 1);

                var to = ReadAt(ptr, len);
                var from = ReadAt(ptr + size, len);

                from.CopyTo(to);
            }

            var node = ReadAt(last, size);
            var entry = new FreeFixedTreeNodeEntry()
            {
                Index = (short)index,
                Buffer = node.ToArray(),
                IsBranch = IsBranch,
                DataSize = fixedHeader.DataElementSize
            };

            node.Clear();
            fixedHeader.Count--;

            return entry;
        }

        public void Search(long target)
        {
            var count = IsLeaf ? FixedHeader.Count : FixedHeader.Count - 1;
            if (count <= 0)
            {
                LastMatch = 0;
                LastMatchIndex = 0;
                return;
            }

            //>last
            if (target > LastKey)
            {
                LastMatch = IsBranch ? 0 : 1;
                LastMatchIndex = IsBranch ? count : count - 1;
                return;
            }

            //<first
            if (target < FirstKey)
            {
                LastMatch = IsBranch ? 0 : -1;
                LastMatchIndex = 0;
                return;
            }

            BinarySearch(target, 0, count - 1);
        }

        public PagePosition SearchPage(long target)
        {
            Search(target);
            return GetLastMatchedPage();
        }

        public void BinarySearch(long target, int first, int last)
        {
            var end = last;
            var start = first;

            while (first < last)
            {
                var mid = first + (last - first) / 2;
                var key = GetNodeKey(mid);
                var flag = target.CompareTo(key);
                if (flag > 0)
                {
                    first = mid + 1;
                }
                else if (flag < 0)
                {
                    last = mid - 1;
                }
                else
                {
                    first = mid + 1;
                }

                LastMatch = flag;
                LastMatchIndex = mid;
            }

            LastMatchIndex = LastMatch == 0 && first > start ? first - 1 : first;

            if (IsBranch)
            {
                LastMatch = 0;
                LastMatchIndex += 1;
            }
            else
            {
                LastMatch = target.CompareTo(GetNodeKey(LastMatchIndex));
            }
        }

        public void CopyEntriesTo(FreeFixedTreePage page, int index)
        {
            ref var fixedHeader = ref FixedHeader;
            var ptr = GetNodePtr(index);
            var count = fixedHeader.Count - index;
            var len = count * GetNodeSize(ref fixedHeader);

            var from = ReadAt(ptr, len);
            var to = ReadAt(Constants.PageHeaderSize, len);

            from.CopyTo(to);
            from.Clear();

            fixedHeader.Count -= (ushort)count;
            page.FixedHeader.Count = (ushort)count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetNodeKey(int index)
        {
            ref var fixedHeader = ref FixedHeader;
            if (fixedHeader.Count == 0)
            {
                return long.MinValue;
            }

            if (index >= fixedHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodePtr(index);
            if (ptr + TreeNodeHeader.SizeOf > Constants.PageSize - Constants.PageTailerSize)
            {
                throw new PageCorruptedException(this);
            }

            return ReadAt<long>(ptr, sizeof(long));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetNodePtr(int index)
        {
            if (index >= FixedHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return (ushort)(index * (FixedHeader.DataElementSize + FreeFixedTreeNodeHeader.SizeOf) + Constants.PageHeaderSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetNodeData(int index)
        {
            ref var fixedHeader = ref FixedHeader;
            if (fixedHeader.Count == 0 || fixedHeader.DataElementSize == 0)
            {
                return Span<byte>.Empty;
            }

            if (index >= fixedHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodePtr(index);
            if (ptr + fixedHeader.DataElementSize + FreeFixedTreeNodeHeader.SizeOf > Constants.PageSize - Constants.PageTailerSize)
            {
                throw new PageCorruptedException(this);
            }

            return ReadAt(ptr, fixedHeader.DataElementSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref FreeFixedTreeNodeHeader GetNodeHeader(ushort ptr)
        {
            return ref ReadAt<FreeFixedTreeNodeHeader>(ptr, FreeFixedTreeNodeHeader.SizeOf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref FreeFixedTreeNodeHeader GetNodeHeader(int index)
        {
            var ptr = GetNodePtr(LastMatchIndex);
            if (ptr > Constants.PageSize - Constants.PageTailerSize ||
                ptr < Constants.PageHeaderSize)
            {
                throw new PageCorruptedException(this);
            }

            return ref GetNodeHeader(ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetNodeSize(ref FreeFixedTreePageHeader fixeHeader)
        {
            return fixeHeader.DataElementSize + FreeFixedTreePageHeader.SizeOf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetUsedSize(ref FreeFixedTreePageHeader fixedHeader)
        {
            return fixedHeader.Count * GetNodeSize(ref fixedHeader) + Constants.PageHeaderSize + Constants.PageTailerSize;
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public PagePosition GetLastMatchedPage()
        {
            if (IsLeaf)
            {
                throw new InvalidOperationException("err api invoke!");
            }

            if (LastMatchIndex == -1)
            {
                throw new InvalidOperationException("must invoke the Search method before inovke the GetLastMatchedPage method!");
            }

            return new PagePosition(Position.FileId, GetNodeHeader(LastMatchIndex).PageNumber);
        }

        public void InitPage(LowLevelTransaction lltx, int fileId, long pageNumber, TreeNodeFlags flags, byte dataElementSize)
        {
            ref var fixedHeader = ref FixedHeader;
            var lsn = fixedHeader.LSN;

            Unsafe.InitBlock(ref Data[0], 0, Constants.PageHeaderSize);

            fixedHeader.Count = 0;
            fixedHeader.FileId = fileId;
            fixedHeader.NodeFlags = flags;
            fixedHeader.DataElementSize = dataElementSize;
            fixedHeader.PageNumber = pageNumber;
            fixedHeader.PrevPageNumber = -1;
            fixedHeader.NextPageNumber = -1;
            fixedHeader.Flags = PageHeaderFlags.BTree;
            fixedHeader.LSN = lsn;
        }
    }
}
