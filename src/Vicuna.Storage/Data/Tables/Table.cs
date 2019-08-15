using System.Collections.Generic;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Tables
{
    public class Table
    {
        public Index Cluster { get; }

        public Dictionary<string, Index> Indexes { get; }

        public PagePosition TableLockPosition => new PagePosition(Cluster.Tree.Root.FileId, -1);
    }
}
