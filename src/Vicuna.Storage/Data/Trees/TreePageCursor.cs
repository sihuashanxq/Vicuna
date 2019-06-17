using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;

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
            LastMatchIndex = 0;
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
            get => GetNodeKey(0);
        }

        public Span<byte> LastKey
        {
            get => GetNodeKey(TreeHeader.Count - (IsLeaf ? 1 : 2));
        }

        public ref TreePageHeader TreeHeader
        {
            get => ref Current.Header.Cast<TreePageHeader>();
        }


        public bool Allocate(int index, ushort size, TreeNodeHeaderFlags flags, out ushort position)
        {
            size += TreeNodeHeader.SizeOf;                                                                  //+ header-size
            size += flags == TreeNodeHeaderFlags.Data ? TreeNodeTransactionHeader.SizeOf : (ushort)0;       //+ trans-header-size

            return AllocateInternal(index, size, flags, out position);
        }

        internal bool AllocateInternal(int index, ushort size, TreeNodeHeaderFlags flags, out ushort position)
        {
            ref var header = ref TreeHeader;
            if (Constants.PageSize - header.UsedSize < size + sizeof(ushort))
            {
                position = 0;
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

                var to = Current.Slice(start + sizeof(ushort), len);
                var from = Current.Slice(start, len);

                from.CopyTo(to);
                Current.Write(start, (ushort)upper);
            }
            else
            {
                Current.Write(header.Low, (ushort)upper);
            }

            position = (ushort)upper;

            header.Low += sizeof(ushort);
            header.Upper = (ushort)upper;
            header.UsedSize += (ushort)(size + sizeof(ushort));
            header.Count++;

            return true;
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
                var nodeKey = GetNodeKey(mid);
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
                        case TreeNodeFetchMode.MoreThanOrEqual:
                            last = mid;
                            break;
                        case TreeNodeFetchMode.LessThan:
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
                LastMatch = CompareNodeKeys(GetNodeKey(LastMatchIndex), key);
            }
        }

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
        public ref TreeNodeHeader GetNodeHeader(ushort pointer)
        {
            return ref Current.Read<TreeNodeHeader>(pointer, TreeNodeHeader.SizeOf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort GetNodeSlot(int index)
        {
            return (ushort)(index * sizeof(ushort) + Constants.PageHeaderSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNode GetNode(int index)
        {
            if (TreeHeader.Count == 0)
            {
                return new TreeNode();
            }

            if (index >= TreeHeader.Count || index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var ptr = GetNodePointer(index);
            var size = (int)TreeNodeHeader.SizeOf;

            ref var node = ref GetNodeHeader(ptr);
            switch (node.NodeFlags)
            {
                case TreeNodeHeaderFlags.Data:
                    size += node.KeySize + TreeNodeTransactionHeader.SizeOf + node.DataSize;
                    break;
                case TreeNodeHeaderFlags.DataNoneTrx:
                    size += node.KeySize + node.DataSize;
                    break;
                default:
                    size += node.KeySize;
                    break;
            }

            if (ptr + size > Constants.PageSize - Constants.PageTailSize)
            {
                throw new IndexOutOfRangeException($"");
            }

            return new TreeNode()
            {
                Slot = GetNodeSlot(index),
                Data = Current.Slice(ptr, size),
                Index = (ushort)index,
                Position = ptr
            };
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
            if (pos + node.KeySize > Constants.PageSize - Constants.PageTailSize)
            {
                throw new IndexOutOfRangeException($@"the node's key pos is out of page's size,page:{Current.Position},index:{index},node-pos:{ptr},key-pos:{pos},key-size:{node.KeySize}");
            }

            return Current.Slice(pos, node.KeySize);
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
                case TreeNodeHeaderFlags.Data:
                    pos = TreeNodeHeader.SizeOf + ptr + node.KeySize + TreeNodeTransactionHeader.SizeOf;
                    break;
                case TreeNodeHeaderFlags.DataNoneTrx:
                    pos = TreeNodeHeader.SizeOf + ptr + node.KeySize;
                    break;
                default:
                    return Span<byte>.Empty;
            }

            if (pos + node.DataSize > Constants.PageSize - Constants.PageTailSize)
            {
                throw new IndexOutOfRangeException($"the node's data pos is out of page's size,page:{Current.Position},index:{index},node-pos:{ptr},data-pos:{pos},data-size:{node.DataSize}");
            }

            return Current.Slice(pos, node.DataSize);
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
                slot > Constants.PageSize - PageTailer.SizeOf)
            {
                throw new IndexOutOfRangeException($"the node's slot is out of page's size,page:{Current.Position},index:{index},slot:{slot}");
            }

            var pointer = Current.Read<ushort>(slot);
            if (pointer < 0 || pointer > Constants.PageSize - Constants.PageTailSize)
            {
                throw new IndexOutOfRangeException($"the node's position is out of page's size,page:{Current.Position},index:{index},slot:{slot},pos:{pointer}");
            }

            return pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareNodeKeys(Span<byte> giveKey, Span<byte> nodeKey)
        {
            return BytesEncodingComparer.Compare(giveKey, nodeKey, StringCompareMode.IgnoreCase);
        }
    }

    public ref struct TreeNode
    {
        public ushort Slot;

        public ushort Index;

        public ushort Position;

        public Span<byte> Data;

        public Span<byte> Key
        {
            get
            {
                return Data.Slice(TreeNodeHeader.SizeOf, Header.KeySize);
            }
        }

        public Span<byte> Value
        {
            get
            {
                switch (Header.NodeFlags)
                {
                    case TreeNodeHeaderFlags.Data:
                        return Data.Slice(TreeNodeHeader.SizeOf + Header.KeySize + TreeNodeTransactionHeader.SizeOf, Header.DataSize);
                    case TreeNodeHeaderFlags.DataNoneTrx:
                        return Data.Slice(TreeNodeHeader.SizeOf + Header.KeySize, Header.DataSize);
                    default:
                        return Span<byte>.Empty;
                }
            }
        }

        public ref TreeNodeHeader Header
        {
            get => ref Unsafe.As<byte, TreeNodeHeader>(ref Data[0]);
        }

        public ref TreeNodeTransactionHeader Transaction
        {
            get
            {
                if (Header.NodeFlags != TreeNodeHeaderFlags.Data)
                {
                    throw new InvalidOperationException($"only data node has tx header!");
                }

                return ref Unsafe.As<byte, TreeNodeTransactionHeader>(ref Data[TreeNodeHeader.SizeOf + Header.KeySize]);
            }
        }
    }
}
