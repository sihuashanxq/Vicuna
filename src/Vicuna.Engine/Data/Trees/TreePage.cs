using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

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

        public bool IsRoot
        {
            get => TreeHeader.NodeFlags.HasFlag(TreeNodeFlags.Root);
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
            if (CompareKey(key, LastKey) > 0)
            {
                LastMatch = IsBranch ? 0 : -1;
                LastMatchIndex = IsBranch ? count : count - 1;
                return this;
            }

            //<first
            if (CompareKey(key, FirstKey) < 0)
            {
                LastMatch = IsBranch ? 0 : 1;
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

        public void BinarySearch(Span<byte> target, int first, int last)
        {
            //var end = last;
            //var start = first;

            //while (first < last)
            //{
            //    var mid = first + (last - first) / 2;
            //    var key = GetNodeKey(mid);
            //    var flag = CompareKey(key, giveKey);
            //    if (flag > 0)
            //    {
            //        first = mid + 1;
            //    }
            //    else if (flag < 0)
            //    {
            //        last = mid - 1;
            //    }
            //    else
            //    {
            //        switch (Mode)
            //        {
            //            case TreeNodeQueryMode.Gt:
            //                first = mid + 1;
            //                break;
            //            case TreeNodeQueryMode.Lt:
            //                last = mid - 1;
            //                break;
            //            case TreeNodeQueryMode.Gte:
            //                last = mid - 1;
            //                break;
            //            case TreeNodeQueryMode.Lte:
            //                first = mid + 1;
            //                break;
            //        }
            //    }

            //    LastMatch = flag;
            //    LastMatchIndex = mid;
            //}

            //switch (Mode)
            //{
            //    case TreeNodeQueryMode.Gt:
            //        LastMatchIndex = first;
            //        break;
            //    case TreeNodeQueryMode.Lt:
            //        LastMatchIndex = last;
            //        break;
            //    case TreeNodeQueryMode.Gte:
            //        LastMatchIndex = LastMatch == 0 && last < end ? last + 1 : last;
            //        break;
            //    default:
            //        LastMatchIndex = LastMatch == 0 && first > start ? first - 1 : first;
            //        break;
            //}

            //if (IsBranch)
            //{
            //    LastMatch = 0;
            //    LastMatchIndex += 1;
            //}
            //else
            //{
            //    LastMatch = CompareKey(giveKey, GetNodeKey(LastMatchIndex));
            //}

            while (first <= last)
            {
                var mid = first + (last - first) / 2;
                var key = GetNodeKey(mid);
                var flag = CompareKey(key, target);//key.CompareTo(target);
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
        public bool AllocForKey(LowLevelTransaction lltx, ref TreeNodeEntryAllocContext ctx, out int matchFlags, out int matchIndex, out TreeNodeEntry entry)
        {
            Search(ctx.Key);

            if (IsLeaf)
            {
                LastMatchIndex = TreeHeader.Count != 0 && LastMatch <= 0 ? LastMatchIndex + 1 : LastMatchIndex;
            }

            matchFlags = LastMatch;
            matchIndex = LastMatchIndex;

            return Alloc(lltx, matchIndex, ref ctx, out entry);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Alloc(LowLevelTransaction lltx, int index, ref TreeNodeEntryAllocContext ctx, out TreeNodeEntry entry)
        {
            ctx.Size += TreeNodeHeader.SizeOf;                                                                     //+ header-size
            ctx.Size += ctx.Flags.HasVersion() ? TreeNodeVersionHeader.SizeOf : (ushort)0;       //+ trans-header-size
            return AllocInternal(lltx, index, ref ctx, out entry);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool AllocInternal(LowLevelTransaction lltx, int index, ref TreeNodeEntryAllocContext ctx, out TreeNodeEntry entry)
        {
            ref var header = ref TreeHeader;
            if (Constants.PageSize - header.UsedSize < ctx.Size + sizeof(ushort))
            {
                entry = TreeNodeEntry.Empty;
                return false;
            }

            var low = header.Low + sizeof(ushort);
            var slot = GetNodeSlot(index);
            var upper = header.Upper - ctx.Size;
            if (upper < low)
            {
                Reorganize(lltx);
                upper = (ushort)(header.Upper - ctx.Size);
            }

            if (index <= header.Count - 1)
            {
                MoveNodeSlots(index, header.Low);
                WriteTo(slot, (ushort)upper);
            }
            else
            {
                WriteTo(header.Low, (ushort)upper);
            }

            header.Low += sizeof(ushort);
            header.Upper = (ushort)upper;
            header.UsedSize += (ushort)(ctx.Size + sizeof(ushort));
            header.Count++;

            entry = CreateNodeEntry(ref ctx, index, upper);

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool AllocInternal(LowLevelTransaction lltx, int index, ushort size, out Span<byte> entry)
        {
            ref var header = ref TreeHeader;
            if (Constants.PageSize - header.UsedSize < size + sizeof(ushort))
            {
                entry = Span<byte>.Empty;
                return false;
            }

            var low = header.Low + sizeof(ushort);
            var slot = GetNodeSlot(index);
            var upper = header.Upper - size;
            if (upper < low)
            {
                Reorganize(lltx);
                upper = (ushort)(header.Upper - size);
            }

            if (index <= header.Count - 1)
            {
                MoveNodeSlots(index, header.Low);
                WriteTo(slot, (ushort)upper);
            }
            else
            {
                WriteTo(header.Low, (ushort)upper);
            }

            header.Low += sizeof(ushort);
            header.Upper = (ushort)upper;
            header.UsedSize += (ushort)(size + sizeof(ushort));
            header.Count++;

            entry = ReadAt(upper, size);
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

            var ptr = GetNodePointer(index);
            var size = (int)TreeNodeHeader.SizeOf;

            ref var node = ref GetNodeHeader(ptr);
            switch (node.NodeFlags)
            {
                case TreeNodeHeaderFlags.Primary:
                    size += node.KeySize + TreeNodeVersionHeader.SizeOf + node.DataSize;
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
                Buffer = ReadAt(ptr, size),
                Index = (short)index
            };

            entry.Key = entry.Buffer.Slice(TreeNodeHeader.SizeOf, entry.Header.KeySize);

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeEntry GetNodeEntry(int index)
        {
            if (TryGetNodeEntry(index, out var entry))
            {
                return entry;
            }

            throw new InvalidOperationException($"can not found the node entry at page:{Position} index:{index}");
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
                    pos = TreeNodeHeader.SizeOf + ptr + node.KeySize + TreeNodeVersionHeader.SizeOf;
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
        /// <param name="from"></param>
        /// <param name="to"></param>
        public void SwitchNodeEntry(int from, int to)
        {
            var toPtr = GetNodePointer(to);
            var toSlot = GetNodeSlot(to);

            var fromPtr = GetNodePointer(from);
            var fromSlot = GetNodeSlot(from);

            WriteTo(toSlot, fromPtr, sizeof(ushort));
            WriteTo(fromSlot, toPtr, sizeof(ushort));
        }

        public void CopyEntriesTo(LowLevelTransaction lltx, TreePage newPage)
        {
            var lsn = newPage.Header.LSN;
            var pos = newPage.Position;

            Data.AsSpan().CopyTo(newPage.Data);

            ref var toHeader = ref newPage.TreeHeader;
            ref var toFooter = ref newPage.Footer;
            ref var fromHeader = ref TreeHeader;

            toFooter.LSN = lsn;
            toHeader.LSN = lsn;
            toHeader.FileId = pos.FileId;
            toHeader.PageNumber = pos.PageNumber;
            toHeader.NodeFlags &= ~TreeNodeFlags.Root;

            fromHeader.Count = 0;
            fromHeader.UsedSize = Constants.PageHeaderSize + Constants.PageFooterSize;
            fromHeader.Low = Constants.PageHeaderSize;
            fromHeader.Upper = Constants.PageSize - Constants.PageFooterSize;
            Data.AsSpan()
                .Slice(Constants.PageHeaderSize, Constants.PageSize - Constants.PageFooterSize - Constants.PageHeaderSize)
                .Clear();
        }

        public int CopyEntriesTo(LowLevelTransaction lltx, int startIndex, TreePage newPage)
        {
            var min = 1;
            ref var header = ref TreeHeader;
            var count = header.Count;
            var start = startIndex < min ? min : startIndex;

            for (var i = count - 1; i >= startIndex; i--)
            {
                if (CopyEntryTo(lltx, i, 0, newPage, true, out var size))
                {
                    header.Low -= sizeof(ushort);
                    header.UsedSize -= size;
                    header.Count--;
                    continue;
                }

                start = i + 1;
                break;
            }

            if (start == startIndex)
            {
                while (start - 1 > min && CopyEntryTo(lltx, start - 1, 0, newPage, false, out var size))
                {
                    header.Low -= sizeof(ushort);
                    header.UsedSize -= size;
                    header.Count--;
                    start--;
                }
            }

            lltx.WriteBTreeCopyEntries(Position, newPage.Position, start);

            return start;
        }

        private bool CopyEntryTo(LowLevelTransaction lltx, int fromIndex, int toIndex, TreePage newPage, bool letCurrentMoreSpace, out ushort nodeSize)
        {
            ref var header = ref TreeHeader;

            if (fromIndex > header.Count - 1)
            {
                nodeSize = 0;
                return false;
            }

            var ptr = GetNodePointer(fromIndex);
            var node = GetNodeHeader(ptr);
            var size = node.GetSize();

            if (letCurrentMoreSpace)
            {
                if (newPage.TreeHeader.UsedSize + size >= header.UsedSize - size)
                {
                    nodeSize = 0;
                    return false;
                }
            }
            else
            {
                if (newPage.TreeHeader.UsedSize + size <= header.UsedSize - size)
                {
                    nodeSize = 0;
                    return false;
                }
            }

            if (!newPage.AllocInternal(lltx, toIndex, (ushort)(size - sizeof(ushort)), out var newEntry))
            {
                throw new Exception("tree page split failed!");
            }

            var oldEntry = ReadAt(ptr, size - sizeof(ushort));

            oldEntry.CopyTo(newEntry);
            nodeSize = size;

            return true;
        }

        /// <summary>
        /// </summary>
        private void Reorganize(LowLevelTransaction lltx)
        {
            ref var header = ref TreeHeader;
            var len = Constants.PageSize - Constants.PageFooterSize - header.Upper;
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

            lltx.WriteBTreePageReorganize(Position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareKey(Span<byte> giveKey, Span<byte> nodeKey)
        {
            return BytesEncodingComparer.Compare(giveKey, nodeKey, StringCompareMode.IgnoreCase);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private TreeNodeEntry CreateNodeEntry(ref TreeNodeEntryAllocContext ctx, int index, int upper)
        {
            var buffer = ReadAt(upper, ctx.Size);
            var hasValue = ctx.Flags.HasValue();
            var hasVersion = ctx.Flags.HasVersion();
            var entry = new TreeNodeEntry()
            {
                Key = buffer.Slice(TreeNodeHeader.SizeOf, ctx.KeySize),
                Index = (short)index,
                Value = Span<byte>.Empty,
                Buffer = buffer,
                Version = Span<byte>.Empty
            };

            if (hasVersion)
            {
                entry.Version = buffer.Slice(TreeNodeHeader.SizeOf + ctx.KeySize, TreeNodeVersionHeader.SizeOf);
            }

            if (hasValue)
            {
                if (hasVersion)
                {
                    entry.Value = buffer.Slice(TreeNodeHeader.SizeOf + ctx.KeySize + TreeNodeVersionHeader.SizeOf, ctx.ValueSize);
                }
                else
                {
                    entry.Value = buffer.Slice(TreeNodeHeader.SizeOf + ctx.KeySize, ctx.ValueSize);
                }
            }

            return entry;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MoveNodeSlots(int index, int low)
        {
            //move slots
            var start = GetNodeSlot(index);
            var len = low - start;

            var to = ReadAt(start + sizeof(ushort), len);
            var from = ReadAt(start, len);

            from.CopyTo(to);
        }

        public ref struct TreeNodeEntryAllocContext
        {
            public ushort Size;

            public Span<byte> Key;

            public ushort KeySize;

            public ushort ValueSize;

            public TreeNodeHeaderFlags Flags;

            public TreeNodeEntryAllocContext(ushort size, ushort keySize, ushort valueSize, TreeNodeHeaderFlags flags)
            {
                Size = size;
                KeySize = keySize;
                ValueSize = valueSize;
                Flags = flags;
                Key = Span<byte>.Empty;
            }
        }
    }
}