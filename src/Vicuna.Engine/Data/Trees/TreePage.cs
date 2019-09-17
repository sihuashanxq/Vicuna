using System;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees
{
    public class TreePage : Page
    {
        public int LastMatch;

        public int LastMatchIndex;

        public TreeNodeQueryMode Mode;

        public TreePage(byte[] buffer, TreeNodeQueryMode mode) : base(buffer)
        {
            Mode = mode;
            LastMatch = 0;
            LastMatchIndex = -1;
        }

        public bool IsLeaf
        {
            get => TreeHeader.NodeFlags.HasFlag(TreeNodeFlags.Leaf);
        }

        public bool IsBranch
        {
            get => TreeHeader.NodeFlags.HasFlag(TreeNodeFlags.Branch);
        }

        public byte Depth
        {
            get => TreeHeader.Depth;
        }

        public Span<byte> FirstKey
        {
            get => GetNodeKey(0);
        }

        public Span<byte> LastKey
        {
            get => GetNodeKey(TreeHeader.Count - (IsLeaf ? 1 : 2));
        }

        public ref TreePageHeader TreeHeader
        {
            get => ref Header.Cast<TreePageHeader>();
        }

        public TreePage Search(Span<byte> key)
        {
            var count = IsLeaf ? TreeHeader.Count : TreeHeader.Count - 1;
            if (count <= 0)
            {
                LastMatch = 0;
                LastMatchIndex = 0;
                return this;
            }

            //>last
            if (CompareKey(LastKey, key) > 0)
            {
                LastMatch = IsBranch ? 0 : 1;
                LastMatchIndex = IsBranch ? count : count - 1;
                return this;
            }

            //<first
            if (CompareKey(FirstKey, key) < 0)
            {
                LastMatch = IsBranch ? 0 : -1;
                LastMatchIndex = 0;
                return this;
            }

            BinarySearch(key, 0, count - 1);
            return this;
        }

        public PagePosition FindPage(Span<byte> key)
        {
            Search(key);
            return GetLastMatchedPage();
        }

        public void BinarySearch(Span<byte> giveKey, int first, int last)
        {
            var end = last;
            var start = first;

            while (first < last)
            {
                var mid = first + (last - first) / 2;
                var key = GetNodeKey(mid);
                var flag = CompareKey(key, giveKey);
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
                    switch (Mode)
                    {
                        case TreeNodeQueryMode.Gt:
                            first = mid + 1;
                            break;
                        case TreeNodeQueryMode.Lt:
                            last = mid - 1;
                            break;
                        case TreeNodeQueryMode.Gte:
                            last = mid - 1;
                            break;
                        case TreeNodeQueryMode.Lte:
                            first = mid + 1;
                            break;
                    }
                }

                LastMatch = flag;
                LastMatchIndex = mid;
            }

            switch (Mode)
            {
                case TreeNodeQueryMode.Gt:
                    LastMatchIndex = first;
                    break;
                case TreeNodeQueryMode.Lt:
                    LastMatchIndex = last;
                    break;
                case TreeNodeQueryMode.Gte:
                    LastMatchIndex = LastMatch == 0 && last < end ? last + 1 : last;
                    break;
                default:
                    LastMatchIndex = LastMatch == 0 && first > start ? first - 1 : first;
                    break;
            }

            if (IsBranch)
            {
                //Debug.Assert(Mode == TreeNodeFetchMode.LessThanOrEqual);
                LastMatch = 0;
                LastMatchIndex += 1;
            }
            else
            {
                LastMatch = CompareKey(giveKey, GetNodeKey(LastMatchIndex));
            }
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

            ref var node = ref GetNodeHeader(LastMatchIndex);
            return new PagePosition(Position.FileId, node.PageNumber);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool AllocForKey(Span<byte> key, ushort size, TreeNodeHeaderFlags flags, out int matchFlags, out int matchIndex, out TreeNodeEntry entry)
        {
            Search(key);

            if (IsLeaf)
            {
                LastMatchIndex = TreeHeader.Count != 0 && LastMatch <= 0 ? LastMatchIndex + 1 : LastMatchIndex;
            }

            matchFlags = LastMatch;
            matchIndex = LastMatchIndex;

            return Alloc(matchIndex, size, flags, out entry);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Alloc(int index, ushort size, TreeNodeHeaderFlags flags, out TreeNodeEntry entry)
        {
            size += TreeNodeHeader.SizeOf;                                                                  //+ header-size
            size += flags == TreeNodeHeaderFlags.Primary ? TreeNodeTransactionHeader.SizeOf : (ushort)0;       //+ trans-header-size
            return AllocInternal(index, size, flags, out entry);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool AllocInternal(int index, ushort size, TreeNodeHeaderFlags flags, out TreeNodeEntry entry)
        {
            ref var header = ref TreeHeader;
            if (Constants.PageSize - header.UsedSize < size + sizeof(ushort))
            {
                entry = TreeNodeEntry.Empty;
                return false;
            }

            var low = header.Low + sizeof(ushort);
            var upper = header.Upper - size;
            if (upper < low)
            {
                Compact();
                upper = (ushort)(header.Upper - size);
            }

            if (index <= header.Count - 1)
            {
                //move slots
                var start = GetNodeSlot(index);
                var len = header.Low - start;

                var to = ReadAt(start + sizeof(ushort), len);
                var from = ReadAt(start, len);

                from.CopyTo(to);
                WriteTo(start, (ushort)upper);
            }
            else
            {
                WriteTo(header.Low, (ushort)upper);
            }

            entry = new TreeNodeEntry()
            {
                Slot = header.Low,
                Data = ReadAt(upper, size),
                Index = (short)index,
                Position = (ushort)upper
            };

            header.Low += sizeof(ushort);
            header.Upper = (ushort)upper;
            header.UsedSize += (ushort)(size + sizeof(ushort));
            header.Count++;

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TreeNodeHeader GetNodeHeader(ushort pos)
        {
            return ref ReadAt<TreeNodeHeader>(pos, TreeNodeHeader.SizeOf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TreeNodeHeader GetNodeHeader(int index)
        {
            var ptr = GetNodePointer(LastMatchIndex);
            if (ptr > Constants.PageSize - Constants.PageFooterSize || ptr < Constants.PageHeaderSize)
            {
                throw new PageDamageException(this);
            }

            return ref GetNodeHeader(ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetNodeSlot(int index)
        {
            return (ushort)(index * sizeof(ushort) + Constants.PageHeaderSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNode(int index, out TreeNodeEntry entry)
        {
            var count = TreeHeader.Count;
            if (count == 0)
            {
                entry = new TreeNodeEntry();
                return false;
            }

            if (index >= count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodePointer(index);
            var size = (int)TreeNodeHeader.SizeOf;

            ref var node = ref GetNodeHeader(ptr);
            switch (node.NodeFlags)
            {
                case TreeNodeHeaderFlags.Primary:
                    size += node.KeySize + TreeNodeTransactionHeader.SizeOf + node.DataSize;
                    break;
                case TreeNodeHeaderFlags.Data:
                    size += node.KeySize + node.DataSize;
                    break;
                default:
                    size += node.KeySize;
                    break;
            }

            if (ptr + size > Constants.PageSize - Constants.PageFooterSize)
            {
                throw new PageDamageException(this);
            }

            entry = new TreeNodeEntry()
            {
                Slot = GetNodeSlot(index),
                Data = ReadAt(ptr, size),
                Index = (short)index,
                Position = ptr
            };

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetNodeKey(int index)
        {
            if (TreeHeader.Count == 0)
            {
                return Span<byte>.Empty;
            }

            if (index >= TreeHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodePointer(index);
            var pos = ptr + TreeNodeHeader.SizeOf;

            ref var node = ref GetNodeHeader(ptr);
            if (pos + node.KeySize > Constants.PageSize - Constants.PageFooterSize)
            {
                throw new PageDamageException(this);
            }

            return ReadAt(pos, node.KeySize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetNodeData(int index)
        {
            if (TreeHeader.Count == 0)
            {
                return Span<byte>.Empty;
            }

            if (index >= TreeHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodePointer(index);
            var pos = 0;

            ref var node = ref GetNodeHeader(ptr);
            switch (node.NodeFlags)
            {
                case TreeNodeHeaderFlags.Primary:
                    pos = TreeNodeHeader.SizeOf + ptr + node.KeySize + TreeNodeTransactionHeader.SizeOf;
                    break;
                case TreeNodeHeaderFlags.Data:
                    pos = TreeNodeHeader.SizeOf + ptr + node.KeySize;
                    break;
                default:
                    return Span<byte>.Empty;
            }

            if (pos + node.DataSize > Constants.PageSize - Constants.PageFooterSize)
            {
                throw new PageDamageException(this);
            }

            return ReadAt(pos, node.DataSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetNodePointer(int index)
        {
            if (index >= TreeHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var slot = GetNodeSlot(index);
            if (slot < TreeNodeHeader.SizeOf ||
                slot > Constants.PageSize - PageFooter.SizeOf)
            {
                throw new IndexOutOfRangeException($"index:{index},slot:{slot},page:{Position}");
            }

            var ptr = ReadAt<ushort>(slot);
            if (ptr < 0 || ptr > Constants.PageSize - Constants.PageFooterSize)
            {
                throw new PageDamageException(this);
            }

            return ptr;
        }

        /// <summary>
        /// </summary>
        private void Compact()
        {
            ref var header = ref TreeHeader;
            var len = Constants.PageSize - header.Upper;
            var index = len;
            var count = header.Count;
            var buffer = new Span<byte>(new byte[len]);
            var to = Span<byte>.Empty;
            var from = Span<byte>.Empty;

            for (var i = 0; i < count; i++)
            {
                var slot = GetNodeSlot(i);
                var ptr = GetNodePointer(i);
                ref var node = ref GetNodeHeader(ptr);
                var size = node.GetSize() - sizeof(ushort);

                index -= size;

                to = buffer.Slice(index, size);
                from = ReadAt(ptr, size);

                from.CopyTo(to);
                WriteTo(slot, (ushort)(header.Upper + index));
            }

            to = ReadAt(header.Upper, len);
            from = buffer;
            from.CopyTo(to);

            header.Upper += (ushort)index;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareKey(Span<byte> giveKey, Span<byte> nodeKey)
        {
            return BytesEncodingComparer.Compare(giveKey, nodeKey, StringCompareMode.IgnoreCase);
        }
    }
}
