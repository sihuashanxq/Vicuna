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
            Logger.Add((byte)LogFlags.MLOG_BEGIN);
        }

        public void WriteFileRaise(int fileId, long length)
        {
            Logger.Add((byte)LogFlags.FILE_RAISE);
            Logger.AddRange(BitConverter.GetBytes(fileId));
            Logger.AddRange(BitConverter.GetBytes(length));
        }

        public void WriteByte1(PagePosition pos, short offset, byte value)
        {
            Logger.Add((byte)LogFlags.SET_BYTE_1);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(offset));
            Logger.Add(value);
        }

        public void WriteByte2(PagePosition pos, short offset, short value)
        {
            Logger.Add((byte)LogFlags.SET_BYTE_2);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(offset));
            Logger.AddRange(BitConverter.GetBytes(value));
        }

        public void WriteByte4(PagePosition pos, short offset, int value)
        {
            Logger.Add((byte)LogFlags.SET_BYTE_4);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(offset));
            Logger.AddRange(BitConverter.GetBytes(value));
        }

        public void WriteByte8(PagePosition pos, short offset, long value)
        {
            Logger.Add((byte)LogFlags.SET_BYTE_8);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(offset));
            Logger.AddRange(BitConverter.GetBytes(value));
        }

        public void WriteBytes(PagePosition pos, short offset, Span<byte> values)
        {
            Logger.Add((byte)LogFlags.SET_BYTES);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(offset));
            Logger.AddRange(BitConverter.GetBytes((short)values.Length));
            Logger.AddRange(values);
        }

        public void WriteBTreeLeafPageInsertEntry()
        {

        }

        public void WriteBTreeLeafPageDeleteEntry()
        {

        }

        public void WriteBTreeBranchPageInsertEntry()
        {

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

        public void WriteBTreeLeafPageCreated(PagePosition pos)
        {

        }

        public void WriteBTreeBranchPageCreated()
        {

        }

        public void WriteBTreePageReorganize(PagePosition pos)
        {
            Logger.Add((byte)LogFlags.BPAGE_REORGANIZE);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
        }

        public void WriteFixedBTreeLeafPageInsertEntry(PagePosition pos, long key, Span<byte> values)
        {
            Logger.Add((byte)LogFlags.FBPAGE_LEAF_INSERT_ENTRY);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(key));
            Logger.Add((byte)values.Length);
            Logger.AddRange(values);
        }

        public void WriteFixedBTreePageDeleteEntry(PagePosition pos, int index)
        {
            Logger.Add((byte)LogFlags.FBPAGE_DELETE_ENTRY);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes((ushort)index));
        }

        public void WriteFixedBTreeBranchPageInsertEntry(PagePosition pos, long key, long lPageNumber, long rPageNumber)
        {
            Logger.Add((byte)LogFlags.FBPAGE_BRANCH_INSERT_ENTRY);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(key));
            Logger.AddRange(BitConverter.GetBytes(lPageNumber));
            Logger.AddRange(BitConverter.GetBytes(rPageNumber));
        }

        public void WriteFixedBTreePageFreed(PagePosition pos)
        {
            Logger.Add((byte)LogFlags.FBPAGE_FREED);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
        }

        public void WriteFixedBTreePageCreated(PagePosition pos, TreeNodeFlags flags, byte depth, byte dataSize)
        {
            Logger.Add((byte)LogFlags.FBPAGE_CREATED);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.Add((byte)flags);
            Logger.Add(depth);
            Logger.Add(dataSize);
        }

        public void WriteFixedBTreeCopyEntries(PagePosition from, PagePosition to, int index)
        {
            Logger.Add((byte)LogFlags.FBPAGE_COPY_ENTRIES);
            Logger.AddRange(BitConverter.GetBytes(from.FileId));
            Logger.AddRange(BitConverter.GetBytes(from.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(to.FileId));
            Logger.AddRange(BitConverter.GetBytes(to.PageNumber));
            Logger.AddRange(BitConverter.GetBytes((short)index));
        }

        public void WriteFixedBTreeRootSplitted(PagePosition pos)
        {
            Logger.Add((byte)LogFlags.FBPAGE_ROOT_SPLITTED);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
        }

        public void WriteFixedBTreeRootInitialized(PagePosition pos)
        {
            Logger.Add((byte)LogFlags.FBPAGE_ROOT_INITED);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
        }

        public void WriteMultiLogEnd()
        {
            Logger.Add((byte)LogFlags.MLOG_END);
        }
    }
}
