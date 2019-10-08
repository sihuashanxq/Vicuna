using System;
using Vicuna.Engine.Data.Trees;
using Vicuna.Engine.Logging;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Transactions
{
    public partial class LowLevelTransaction
    {
        public void WriteMultiLogBegin()
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.MLOG_BEGIN);
            }
        }

        public void WriteFileRaise(int fileId, long length)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FILE_RAISE);
                Logger.AddRange(BitConverter.GetBytes(fileId));
                Logger.AddRange(BitConverter.GetBytes(length));
            }
        }

        public void WriteByte1(PagePosition pos, short offset, byte value)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.SET_BYTE_1);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(offset));
                Logger.Add(value);

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteByte2(PagePosition pos, short offset, short value)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.SET_BYTE_2);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(offset));
                Logger.AddRange(BitConverter.GetBytes(value));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteByte4(PagePosition pos, short offset, int value)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.SET_BYTE_4);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(offset));
                Logger.AddRange(BitConverter.GetBytes(value));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteByte8(PagePosition pos, short offset, long value)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.SET_BYTE_8);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(offset));
                Logger.AddRange(BitConverter.GetBytes(value));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteBytes(PagePosition pos, short offset, Span<byte> values)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.SET_BYTES);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(offset));
                Logger.AddRange(BitConverter.GetBytes((short)values.Length));
                Logger.AddRange(values);

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteBTreeLeafPageInsertEntry(PagePosition pos, TreeNodeHeaderFlags nodeFlags, Span<byte> key, Span<byte> value)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.BPAGE_LEAF_INSERT_ENTRY);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.Add((byte)nodeFlags);
                Logger.AddRange(key);
                Logger.AddRange(value);

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteBTreeLeafPageDeleteEntry()
        {

        }

        public void WriteBTreeBranchPageInsertEntry(PagePosition pos, Span<byte> key, long lPageNumber, long rPageNumber)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.BPAGE_BRANCH_INSERT_ENTRY);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(key);
                Logger.AddRange(BitConverter.GetBytes(lPageNumber));
                Logger.AddRange(BitConverter.GetBytes(rPageNumber));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteBTreeBranchPageDeleteEntry()
        {

        }

        public void WriteBTreeBranchPageFreed()
        {

        }

        public void WriteBTreeLeafPageFreed(PagePosition pos)
        {

        }

        public void WriteBTreePageCreated(PagePosition pos, TreeNodeFlags flags, byte depth)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.BPAGE_CREATED);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.Add((byte)flags);
                Logger.Add(depth);

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteBTreeCopyEntries(PagePosition from, PagePosition to, int index)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.BPAGE_COPY_ENTRIES);
                Logger.AddRange(BitConverter.GetBytes(from.FileId));
                Logger.AddRange(BitConverter.GetBytes(from.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(to.FileId));
                Logger.AddRange(BitConverter.GetBytes(to.PageNumber));
                Logger.AddRange(BitConverter.GetBytes((short)index));

                Modified = true;
                Modifies.Add(from);
                Modifies.Add(to);
            }
        }

        public void WriteBTreePageReorganize(PagePosition pos)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.BPAGE_REORGANIZE);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteFixedBTreeLeafPageInsertEntry(PagePosition pos, long key, Span<byte> values)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FBPAGE_LEAF_INSERT_ENTRY);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(key));
                Logger.Add((byte)values.Length);
                Logger.AddRange(values);

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteFixedBTreePageDeleteEntry(PagePosition pos, int index)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FBPAGE_DELETE_ENTRY);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(BitConverter.GetBytes((ushort)index));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteFixedBTreeBranchPageInsertEntry(PagePosition pos, long key, long lPageNumber, long rPageNumber)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FBPAGE_BRANCH_INSERT_ENTRY);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(key));
                Logger.AddRange(BitConverter.GetBytes(lPageNumber));
                Logger.AddRange(BitConverter.GetBytes(rPageNumber));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteFixedBTreePageFreed(PagePosition pos)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FBPAGE_FREED);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteFixedBTreePageCreated(PagePosition pos, TreeNodeFlags flags, byte depth, byte dataSize)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FBPAGE_CREATED);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
                Logger.Add((byte)flags);
                Logger.Add(depth);
                Logger.Add(dataSize);

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteFixedBTreeCopyEntries(PagePosition from, PagePosition to, int index)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FBPAGE_COPY_ENTRIES);
                Logger.AddRange(BitConverter.GetBytes(from.FileId));
                Logger.AddRange(BitConverter.GetBytes(from.PageNumber));
                Logger.AddRange(BitConverter.GetBytes(to.FileId));
                Logger.AddRange(BitConverter.GetBytes(to.PageNumber));
                Logger.AddRange(BitConverter.GetBytes((short)index));

                Modified = true;
                Modifies.Add(from);
                Modifies.Add(to);
            }
        }

        public void WriteFixedBTreeRootSplitted(PagePosition pos)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FBPAGE_ROOT_SPLITTED);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteFixedBTreeRootInitialized(PagePosition pos)
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.FBPAGE_ROOT_INITED);
                Logger.AddRange(BitConverter.GetBytes(pos.FileId));
                Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));

                Modified = true;
                Modifies.Add(pos);
            }
        }

        public void WriteMultiLogEnd()
        {
            if (LogEnable)
            {
                Logger.Add((byte)LogFlags.MLOG_END);
            }
        }
    }
}
