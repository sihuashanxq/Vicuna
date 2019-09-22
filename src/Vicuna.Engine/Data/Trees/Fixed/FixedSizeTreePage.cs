using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public class FixedSizeTreePage : Page
    {
        public int LastMatch;

        public int LastMatchIndex;

        public FixedSizeTreePage(byte[] data) : base(data)
        {
            LastMatch = 0;
            LastMatchIndex = -1;
        }

        public byte Depth
        {
            get => FixedHeader.Depth;
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

        public ref FixedSizeTreeHeader FixedHeader
        {
            get => ref Header.Cast<FixedSizeTreeHeader>();
        }

        public bool Alloc(int index, out FixedSizeTreeNodeEntry entry)
        {
            ref var fixedHeader = ref FixedHeader;
            var ptr = GetNodePtr(index);
            var usedSize = GetUsedSize(ref fixedHeader);
            var nodeSize = GetNodeSize(ref fixedHeader);
            if (nodeSize + usedSize > Constants.PageSize)
            {
                entry = FixedSizeTreeNodeEntry.Empty;
                return false;
            }

            if (index <= fixedHeader.Count - 1)
            {
                var len = (fixedHeader.Count - index) * nodeSize;
                var to = ReadAt(ptr + nodeSize, len);
                var from = ReadAt(ptr, len);

                from.CopyTo(to);
            }

            entry = new FixedSizeTreeNodeEntry()
            {
                Index = (short)index,
                Buffer = ReadAt(ptr, nodeSize),
                DataSize = fixedHeader.DataElementSize,
                IsBranch = fixedHeader.NodeFlags.HasFlag(TreeNodeFlags.Branch)
            };

            fixedHeader.Count++;
            return true;
        }

        public bool AllocForKey(long key, out int matchFlags, out int matchIndex, out FixedSizeTreeNodeEntry entry)
        {
            Search(key);

            if (IsLeaf)
            {
                LastMatchIndex = FixedHeader.Count != 0 && LastMatch <= 0 ? LastMatchIndex + 1 : LastMatchIndex;
            }

            matchFlags = LastMatch;
            matchIndex = LastMatchIndex;

            return Alloc(matchIndex, out entry);
        }

        public FixedSizeTreeNodeEntry RemoveEntry(LowLevelTransaction lltx, int index)
        {
            ref var fixedHeader = ref FixedHeader;
            if (index < 0 || index > fixedHeader.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodePtr(index);
            var size = GetNodeSize(ref fixedHeader);
            var last = GetNodePtr(fixedHeader.Count - 1);
            var node = ReadAt(ptr, size);
            var entry = new FixedSizeTreeNodeEntry()
            {
                Index = (short)index,
                Buffer = node.ToArray(),
                IsBranch = IsBranch,
                DataSize = fixedHeader.DataElementSize
            };

            if (index != fixedHeader.Count - 1)
            {
                var len = size * (fixedHeader.Count - 1);
                var to = ReadAt(ptr, len);
                var from = ReadAt(ptr + size, len);

                from.CopyTo(to);
            }

            ReadAt(last, size).Clear();
            fixedHeader.Count--;

            lltx.WriteFixedBTreePageDeleteEntry(Position, index);
            return entry;
        }

        public void Search(long key)
        {
            var count = IsLeaf ? FixedHeader.Count : FixedHeader.Count - 1;
            if (count <= 0)
            {
                LastMatch = 1;
                LastMatchIndex = 0;
                return;
            }

            //>last
            if (key > LastKey)
            {
                LastMatch = IsBranch ? 0 : -1;
                LastMatchIndex = IsBranch ? count : count - 1;
                return;
            }

            //<first
            if (key < FirstKey)
            {
                LastMatch = IsBranch ? 0 : 1;
                LastMatchIndex = 0;
                return;
            }

            BinarySearch(key, 0, count - 1);
        }

        public PagePosition FindPage(long key)
        {
            Search(key);
            return GetLastMatchedPage();
        }

        public void BinarySearch(long target, int first, int last)
        {
            while (first <= last)
            {
                var mid = first + (last - first) / 2;
                var key = GetNodeKey(mid);
                var flag = key.CompareTo(target);
                if (flag == 0)
                {
                    LastMatch = 0;
                    LastMatchIndex = mid;
                    break;
                }

                if (flag == -1)
                {
                    first = mid + 1;
                    LastMatch = -1;
                    LastMatchIndex = mid;
                }
                else
                {
                    last = mid - 1;
                    LastMatch = 1;
                    LastMatchIndex = mid;
                }
            }

            if (IsBranch)
            {
                LastMatchIndex = LastMatch <= 0 ? LastMatchIndex + 1 : LastMatchIndex;
                LastMatch = 0;
            }
        }

        public void CopyEntriesTo(LowLevelTransaction lltx, int startIndex, FixedSizeTreePage newPage)
        {
            ref var fixedHeader = ref FixedHeader;
            var ptr = GetNodePtr(startIndex);
            var count = fixedHeader.Count - startIndex;
            var len = count * GetNodeSize(ref fixedHeader);

            var from = ReadAt(ptr, len);
            var to = newPage.ReadAt(Constants.PageHeaderSize, len);

            from.CopyTo(to);
            from.Clear();

            fixedHeader.Count -= (ushort)count;
            newPage.FixedHeader.Count = (ushort)count;

            lltx.WriteFixedBTreeCopyEntries(Position, newPage.Position, startIndex);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetNodeKey(int index)
        {
            ref var fixedHeader = ref FixedHeader;
            if (fixedHeader.Count == 0)
            {
                return long.MinValue;
            }

            var ptr = GetNodePtr(index);
            if (ptr + FixedSizeTreeNodeHeader.SizeOf > Constants.PageSize - Constants.PageFooterSize)
            {
                throw new PageDamageException(this);
            }

            return ReadAt<FixedSizeTreeNodeHeader>(ptr, FixedSizeTreeNodeHeader.SizeOf).PageNumber;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetNodePtr(int index)
        {
            return (ushort)(index * (FixedHeader.DataElementSize + FixedSizeTreeNodeHeader.SizeOf) + Constants.PageHeaderSize);
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

            var ptr = GetNodePtr(index) + FixedSizeTreeNodeHeader.SizeOf;
            if (ptr + fixedHeader.DataElementSize > Constants.PageSize - Constants.PageFooterSize)
            {
                throw new PageDamageException(this);
            }

            return ReadAt(ptr, fixedHeader.DataElementSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref FixedSizeTreeNodeHeader GetNodeHeader(ushort ptr)
        {
            return ref ReadAt<FixedSizeTreeNodeHeader>(ptr, FixedSizeTreeNodeHeader.SizeOf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref FixedSizeTreeNodeHeader GetNodeHeader(int index)
        {
            var ptr = GetNodePtr(index);
            if (ptr > Constants.PageSize - Constants.PageFooterSize ||
                ptr < Constants.PageHeaderSize)
            {
                throw new PageDamageException(this);
            }

            return ref GetNodeHeader(ptr);
        }

        public FixedSizeTreeNodeEntry GetNodeEntry(int index)
        {
            ref var fixedHeader = ref FixedHeader;
            var ptr = GetNodePtr(index);
            if (ptr > Constants.PageSize - Constants.PageFooterSize ||
                ptr < Constants.PageHeaderSize)
            {
                throw new PageDamageException(this);
            }

            return new FixedSizeTreeNodeEntry()
            {
                Index = (short)index,
                Buffer = ReadAt(ptr, GetNodeSize(ref fixedHeader)),
                DataSize = fixedHeader.DataElementSize,
                IsBranch = IsBranch
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetNodeSize(ref FixedSizeTreeHeader fixeHeader)
        {
            return fixeHeader.DataElementSize + FixedSizeTreeNodeHeader.SizeOf;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetUsedSize(ref FixedSizeTreeHeader fixedHeader)
        {
            return fixedHeader.Count * GetNodeSize(ref fixedHeader) + Constants.PageHeaderSize + Constants.PageFooterSize;
        }

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

            var data = GetNodeData(LastMatchIndex);
            var pageNumber = Unsafe.As<byte, long>(ref data[0]);

            return new PagePosition(Position.FileId, pageNumber);
        }
    }
}
