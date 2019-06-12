using System;
using Vicuna.Storage.Paging;
using Vicuna.Storage.Buffers;

namespace Vicuna.Storage.Data.Trees
{
    public class TreePage
    {
        internal PageBufferEntry Buffer;

        internal ref TreePageHeader Header => ref Buffer.Page.GetHeader<TreePageHeader>(TreePageHeader.SizeOf);

        //internal ref PageTailer Tailer => ref Page.Tailer;

        //internal ref TreePageHeader Header => ref Page.Read<TreePageHeader>(0, TreePageHeader.SizeOf);
    }
}
