using System;
using System.Collections.Generic;
using System.Text;
using Vicuna.Engine.Buffers;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        public TreePageCursor SplitPage(LowLevelTransaction tx, TreePageCursor cursor, Span<byte> key, int index)
        {
            return null;
        }
    }
}
