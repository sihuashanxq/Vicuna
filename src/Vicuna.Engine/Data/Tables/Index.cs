using Vicuna.Engine.Data.Trees;

namespace Vicuna.Engine.Data.Tables
{
    public class Index
    {
        public Tree Tree { get; }

        public Table Table { get; }

        public bool IsUnique { get; }

        public bool IsCluster { get; }
    }
}
