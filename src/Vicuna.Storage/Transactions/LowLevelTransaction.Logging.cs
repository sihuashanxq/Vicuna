using System;
using Vicuna.Engine.Data.Trees;
using Vicuna.Engine.Logging;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Transactions
{
    public partial class LowLevelTransaction
    {
        public void WriteLogBegin()
        {
            Logger.Add((byte)LogFlags.LOG_BEGIN);
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
            //AddRange(values);
        }

        public void WriteBTreeLeafPageFree(PagePosition pos)
        {

        }

        public void WriteBTreeLeafPageCreated(PagePosition pos)
        {

        }

        public void WriteBTreeLeafPageInsert()
        {

        }

        public void WriteBTreeLeafPageDelete()
        {

        }

        public void WriteBTreeBranchPageFree()
        {

        }

        public void WriteBTreeBranchPageCreate()
        {

        }

        public void WriteBTreeBranchPageInsert()
        {

        }

        public void WriteBTreeBranchPageDelete()
        {

        }

        public void WriteBTreePageReorganize(PagePosition pos)
        {
            Logger.Add((byte)LogFlags.BPAGE_REORGANIZE);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
        }

        public void WriteFixedTreeLeafPageFree(PagePosition pos)
        {
            Logger.Add((byte)LogFlags.FPAGE_LEAF_FREE);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
        }

        public void WriteFixedTreeLeafPageCreate(PagePosition pos, TreeNodeFlags flags, byte depth, byte dataSize)
        {
            Logger.Add((byte)LogFlags.FPAGE_LEAF_CREATE);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.Add((byte)flags);
            Logger.Add(depth);
            Logger.Add(dataSize);
        }

        public void WriteFixedTreeLeafPageInsert(PagePosition pos, long key, Span<byte> values)
        {
            Logger.Add((byte)LogFlags.FPAGE_LEAF_INSERT);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(key));
            Logger.Add((byte)values.Length);
            //AddRange(values);
        }

        public void WriteFixedTreeLeafPageDelete(PagePosition pos, short index)
        {
            Logger.Add((byte)LogFlags.FPAGE_LEAF_DELETE);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(index));
        }

        public void WriteFixedTreeBranchPageFree()
        {

        }

        public void WriteFixedTreeBranchPageCreate()
        {

        }

        public void WriteFixedTreeBranchPageInsert(PagePosition pos, long key, long lPageNumber, long rPageNumber)
        {
            Logger.Add((byte)LogFlags.FPAGE_BRANCH_INSERT);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(key));
            Logger.AddRange(BitConverter.GetBytes(lPageNumber));
            Logger.AddRange(BitConverter.GetBytes(rPageNumber));
        }

        public void WriteFixedTreeBranchPageDelete(PagePosition pos, short index)
        {
            Logger.Add((byte)LogFlags.FPAGE_BRANCH_DELETE);
            Logger.AddRange(BitConverter.GetBytes(pos.FileId));
            Logger.AddRange(BitConverter.GetBytes(pos.PageNumber));
            Logger.AddRange(BitConverter.GetBytes(index));
        }

        public void WriteLogEnd()
        {
            Logger.Add((byte)LogFlags.LOG_END);
        }
    }
}
