using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public struct TreePageCursor
    {
        public Page Current { get; set; }

        public int LastMatch { get; set; }

        public int LastMatchIndex { get; set; }

        public TreeNodeFetchMode Mode { get; set; }

        public TreePageCursor(Page page, TreeNodeFetchMode mode)
        {
            Mode = mode;
            Current = page;
            LastMatch = 0;
            LastMatchIndex = -1;
        }

        public int Level
        {
            get => TreeHeader.Level;
        }

        public bool IsLeaf
        {
            get => TreeHeader.NodeFlags.HasFlag(TreeNodeFlags.Leaf);
        }

        public bool IsBranch
        {
            get => TreeHeader.NodeFlags.HasFlag(TreeNodeFlags.Branch);
        }

        public Span<byte> FirstKey
        {
            get => GetNodeEntryKey(0);
        }

        public Span<byte> LastKey
        {
            get => GetNodeEntryKey(TreeHeader.Count - (IsLeaf ? 1 : 2));
        }

        public ref TreePageHeader TreeHeader
        {
            get => ref Current.Header.Cast<TreePageHeader>();
        }

        public void Search(Span<byte> key)
        {
            var count = IsLeaf ? TreeHeader.Count : TreeHeader.Count - 1;
            if (count <= 0)
            {
                LastMatch = 0;
                LastMatchIndex = 0;
                return;
            }

            //>last
            if (CompareNodeKeys(key, LastKey) > 0)
            {
                LastMatch = IsBranch ? 0 : 1;
                LastMatchIndex = IsBranch ? count : count - 1;
                return;
            }

            //<first
            if (CompareNodeKeys(key, FirstKey) < 0)
            {
                LastMatch = IsBranch ? 0 : -1;
                LastMatchIndex = 0;
                return;
            }

            BinarySearch(key, 0, count - 1);
        }

        public void BinarySearch(Span<byte> key, int first, int last)
        {
            while (first < last - 1)
            {
                var mid = first + (last - first) / 2;
                var nodeKey = GetNodeEntryKey(mid);
                var flag = CompareNodeKeys(key, nodeKey);
                if (flag > 0)
                {
                    first = mid;
                }
                else if (flag < 0)
                {
                    last = mid;
                }
                else
                {
                    switch (Mode)
                    {
                        case TreeNodeFetchMode.MoreThan:
                            first = mid;
                            break;
                        case TreeNodeFetchMode.LessThan:
                            last = mid;
                            break;
                        case TreeNodeFetchMode.MoreThanOrEqual:
                            last = mid;
                            break;
                        case TreeNodeFetchMode.LessThanOrEqual:
                            first = mid;
                            break;
                    }
                }

                LastMatch = flag;
                LastMatchIndex = mid;
            }

            switch (Mode)
            {
                case TreeNodeFetchMode.MoreThan:
                case TreeNodeFetchMode.MoreThanOrEqual:
                    LastMatchIndex = last;
                    break;
                default:
                    LastMatchIndex = first;
                    break;
            }

            if (IsBranch)
            {
                Debug.Assert(Mode == TreeNodeFetchMode.LessThanOrEqual);
                LastMatch = 0;
                LastMatchIndex += 1;
            }
            else
            {
                LastMatch = CompareNodeKeys(GetNodeEntryKey(LastMatchIndex), key);
            }
        }

        public bool TryGetLastMatchedPageNumber(out long pageNumber)
        {
            if (LastMatchIndex < 0)
            {
                pageNumber = -1;
                return false;
            }

            var ptr = GetNodeEntryPointer(LastMatchIndex);
            pageNumber = GetNodeEntryHeader(ptr).PageNumber;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Allocate(int index, ushort size, TreeNodeHeaderFlags flags, out TreeNodeEntry entry)
        {
            size += TreeNodeHeader.SizeOf;                                                                  //+ header-size
            size += flags == TreeNodeHeaderFlags.Data ? TreeNodeTransactionHeader.SizeOf : (ushort)0;       //+ trans-header-size
            return AllocateInternal(index, size, flags, out entry);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool AllocateInternal(int index, ushort size, TreeNodeHeaderFlags flags, out TreeNodeEntry entry)
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
                var start = GetNodeEntrySlot(index);
                var len = header.Low - start;

                var to = Current.Slice(start + sizeof(ushort), len);
                var from = Current.Slice(start, len);

                from.CopyTo(to);
                Current.Write(start, (ushort)upper);
            }
            else
            {
                Current.Write(header.Low, (ushort)upper);
            }

            entry = new TreeNodeEntry()
            {
                Slot = header.Low,
                Data = Current.Slice(upper, size),
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
        public ref TreeNodeHeader GetNodeEntryHeader(ushort pointer)
        {
            return ref Current.Read<TreeNodeHeader>(pointer, TreeNodeHeader.SizeOf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetNodeEntrySlot(int index)
        {
            return (ushort)(index * sizeof(ushort) + Constants.PageHeaderSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNodeEntry(int index, out TreeNodeEntry entry)
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

            var ptr = GetNodeEntryPointer(index);
            var size = (int)TreeNodeHeader.SizeOf;

            ref var node = ref GetNodeEntryHeader(ptr);
            switch (node.NodeFlags)
            {
                case TreeNodeHeaderFlags.Data:
                    size += node.KeySize + TreeNodeTransactionHeader.SizeOf + node.DataSize;
                    break;
                case TreeNodeHeaderFlags.DataRefrence:
                    size += node.KeySize + node.DataSize;
                    break;
                default:
                    size += node.KeySize;
                    break;
            }

            if (ptr + size > Constants.PageSize - Constants.PageTailSize)
            {
                throw new PageCorruptedException(Current);
            }

            entry = new TreeNodeEntry()
            {
                Slot = GetNodeEntrySlot(index),
                Data = Current.Slice(ptr, size),
                Index = (short)index,
                Position = ptr
            };

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetNodeEntryPointer(int index)
        {
            if (index >= TreeHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var slot = GetNodeEntrySlot(index);
            if (slot < TreeNodeHeader.SizeOf ||
                slot > Constants.PageSize - PageTailer.SizeOf)
            {
                throw new PageCorruptedException(Current);
            }

            var ptr = Current.Read<ushort>(slot);
            if (ptr < 0 || ptr > Constants.PageSize - Constants.PageTailSize)
            {
                throw new PageCorruptedException(Current);
            }

            return ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetNodeEntryKey(int index)
        {
            if (TreeHeader.Count == 0)
            {
                return Span<byte>.Empty;
            }

            if (index >= TreeHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodeEntryPointer(index);
            var pos = ptr + TreeNodeHeader.SizeOf;

            ref var node = ref GetNodeEntryHeader(ptr);
            if (pos + node.KeySize > Constants.PageSize - Constants.PageTailSize)
            {
                throw new PageCorruptedException(Current);
            }

            return Current.Slice(pos, node.KeySize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetNodeEntryValue(int index)
        {
            if (TreeHeader.Count == 0)
            {
                return Span<byte>.Empty;
            }

            if (index >= TreeHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodeEntryPointer(index);
            var pos = 0;

            ref var node = ref GetNodeEntryHeader(ptr);
            switch (node.NodeFlags)
            {
                case TreeNodeHeaderFlags.Data:
                    pos = TreeNodeHeader.SizeOf + ptr + node.KeySize + TreeNodeTransactionHeader.SizeOf;
                    break;
                case TreeNodeHeaderFlags.DataRefrence:
                    pos = TreeNodeHeader.SizeOf + ptr + node.KeySize;
                    break;
                default:
                    return Span<byte>.Empty;
            }

            if (pos + node.DataSize > Constants.PageSize - Constants.PageTailSize)
            {
                throw new PageCorruptedException(Current);
            }

            return Current.Slice(pos, node.DataSize);
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
                var slot = GetNodeEntrySlot(i);
                var ptr = GetNodeEntryPointer(i);
                ref var node = ref GetNodeEntryHeader(ptr);
                var size = node.GetSize() - sizeof(ushort);

                index -= size;

                to = buffer.Slice(index, size);
                from = Current.Slice(ptr, size);

                from.CopyTo(to);
                Current.Write(slot, (ushort)(header.Upper + index));
            }

            to = Current.Slice(header.Upper, len);
            from = buffer;
            from.CopyTo(to);

            header.Upper += (ushort)index;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareNodeKeys(Span<byte> giveKey, Span<byte> nodeKey)
        {
            return BytesEncodingComparer.Compare(giveKey, nodeKey, StringCompareMode.IgnoreCase);
        }
    }
}
