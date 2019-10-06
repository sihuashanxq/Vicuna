using System.Collections.Generic;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Tables
{
    public class Table
    {
        public TableIndex Cluster { get; }

        public Dictionary<string, TableIndex> Indexes { get; }

        public PagePosition TableLockPosition => new PagePosition(Cluster.Tree.Root.FileId, -1);
    }
}
