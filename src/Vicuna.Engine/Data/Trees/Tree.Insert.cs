using System;
using Vicuna.Engine.Transactions;
using static Vicuna.Engine.Data.Trees.TreePage;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        public DBOperationFlags AddClusterEntry(LowLevelTransaction tx, KVTuple kv)
        {
            var page = GetPageForUpdate(tx, kv.Key, Constants.BTreeLeafPageDepth);
            return AddClusterEntry(tx, page, kv);
        }

        protected DBOperationFlags AddClusterEntry(LowLevelTransaction lltx, TreePage page, KVTuple kv)
        {
            if (kv.Length > MaxEntrySizeInPage)
            {
                return AddClusterOverflowEntry(lltx, page, kv);
            }

            var nodeCtx = new TreeNodeEntryAllocContext()
            {
                Key = kv.Key,
                Size = (ushort)kv.Length,
                KeySize = (ushort)kv.Key.Length,
                ValueSize = (ushort)kv.Value.Length,
                Flags = TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary,
            };

            if (page.AllocForKey(lltx, ref nodeCtx, out var matchFlags, out var matchIndex, out var entry))
            {
                ref var header = ref entry.Header;

                header.IsDeleted = false;
                header.KeySize = (ushort)kv.Key.Length;
                header.DataSize = (ushort)kv.Value.Length;
                header.NodeFlags = TreeNodeHeaderFlags.Data | TreeNodeHeaderFlags.Primary;

                kv.Key.CopyTo(entry.Key);
                kv.Value.CopyTo(entry.Value);

                ref var versionHeader = ref entry.VersionHeader;

                versionHeader.TransactionNumber = lltx.Id;
                versionHeader.TransactionRollbackNumber = -1;

                lltx.WriteBTreeLeafPageInsertEntry(page.Position, TreeNodeHeaderFlags.Primary, kv.Key, kv.Value);

                return DBOperationFlags.Ok;
            }

            var ctx = SplitPage(lltx, page, kv.Key, matchIndex);
            if (ctx.Index >= matchIndex)
            {
                return AddClusterEntry(lltx, ctx.Current, kv);
            }

            return AddClusterEntry(lltx, ctx.Sibling, kv);
        }

        protected DBOperationFlags AddClusterOverflowEntry(LowLevelTransaction tx, TreePage page, KVTuple kv)
        {
            return DBOperationFlags.Ok;
        }

        protected void AddBranchEntry(LowLevelTransaction lltx, TreePage page, long lPageNumber, long rPageNumber, Span<byte> key)
        {
            ref var treeHeader = ref page.TreeHeader;
            if (!treeHeader.NodeFlags.HasFlag(TreeNodeFlags.Branch))
            {
                throw new InvalidOperationException($"page:{page.Position} is not a branch page!");
            }

            if (treeHeader.Count == 0)
            {
                var ctx0 = new TreeNodeEntryAllocContext((ushort)key.Length, (ushort)key.Length, 0, TreeNodeHeaderFlags.Page);
                var ctx1 = new TreeNodeEntryAllocContext(0, 0, 0, TreeNodeHeaderFlags.Page);

                page.Alloc(lltx, 0, ref ctx0, out var entry1);
                page.Alloc(lltx, 1, ref ctx1, out var entry2);

                key.CopyTo(entry1.Key);
                entry1.Header.KeySize = (ushort)key.Length;
                entry1.Header.PageNumber = lPageNumber;
                entry2.Header.PageNumber = rPageNumber;

                //lltx.WriteFixedBTreeBranchPageInsertEntry(page.Position, key, lPageNumber, rPageNumber);
                return;
            }

            var nodeCtx = new TreeNodeEntryAllocContext()
            {
                Key = key,
                ValueSize = 0,
                Flags = TreeNodeHeaderFlags.Page,
                KeySize = (ushort)key.Length,
                Size = (ushort)key.Length
            };

            if (page.AllocForKey(lltx, ref nodeCtx, out _, out var index, out var entry))
            {
                if (index == treeHeader.Count - 1)
                {
                    var prevEntry = page.GetNodeEntry(index - 1);

                    key.CopyTo(entry.Key);

                    entry.Header.KeySize = (ushort)key.Length;
                    entry.Header.PageNumber = lPageNumber;
                    prevEntry.Header.PageNumber = rPageNumber;

                    page.SwitchNodeEntry(index - 1, index);
                }
                else
                {
                    var nextEntry = page.GetNodeEntry(index + 1);

                    key.CopyTo(entry.Key);

                    entry.Header.KeySize = (ushort)key.Length;
                    entry.Header.PageNumber = lPageNumber;
                    nextEntry.Header.PageNumber = rPageNumber;
                }

                lltx.WriteBTreeBranchPageInsertEntry(page.Position, key, lPageNumber, rPageNumber);
            }
            else
            {
                var ctx = SplitBranch(lltx, page, treeHeader.Count / 2);
                if (ctx.Index <= index)
                {
                    AddBranchEntry(lltx, ctx.Sibling, lPageNumber, rPageNumber, key);
                }
                else
                {
                    AddBranchEntry(lltx, ctx.Current, lPageNumber, rPageNumber, key);
                }
            }
        }
    }
}