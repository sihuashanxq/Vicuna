using System.Collections.Generic;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Tables
{
    public class Table
    {
        public Index Primary { get; }

        public Dictionary<string, Index> Indexes { get; }

        public PagePosition TableLockPosition => new PagePosition(Primary.Tree.Root.FileId, -1);
    }
}
