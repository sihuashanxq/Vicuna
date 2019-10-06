using System;
using System.Collections;
using System.Collections.Generic;
using Vicuna.Engine.Transactions;
using static Vicuna.Engine.Data.Trees.TreePage;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        public DBOperationFlags AddOpmtClusterEntry(LowLevelTransaction lltx, KVTuple kv, TreeNodeHeaderFlags nodeFlags)
        {
            var page = GetPageForUpdate(lltx, kv.Key, Constants.BTreeLeafPageDepth);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            if (kv.Length > MaxEntrySizeInPage)
            {
                return AddClusterOverflowEntry(lltx, page, kv);
            }

            var ctx = new TreeNodeEntryAllocContext()
            {
                Key = kv.Key,
                Size = (ushort)kv.Length,
                KeySize = (ushort)kv.Key.Length,
                ValueSize = (ushort)kv.Value.Length,
                NodeFlags = nodeFlags,
            };

            if (page.AllocForKey(lltx, ref ctx, out var _, out var _, out var entry))
            {
                ref var header = ref entry.Header;

                header.IsDeleted = false;
                header.KeySize = (ushort)kv.Key.Length;
                header.DataSize = (ushort)kv.Value.Length;
                header.NodeFlags = nodeFlags;

                kv.Key.CopyTo(entry.Key);
                kv.Value.CopyTo(entry.Value);

                ref var versionHeader = ref entry.VersionHeader;

                versionHeader.TransactionNumber = lltx.Id;
                versionHeader.TransactionRollbackNumber = -1;

                lltx.WriteBTreeLeafPageInsertEntry(page.Position, nodeFlags, kv.Key, kv.Value);

                return DBOperationFlags.Ok;
            }

            return DBOperationFlags.Split;
        }

        public DBOperationFlags AddPsmtClusterEntry(LowLevelTransaction lltx, KVTuple kv, TreeNodeHeaderFlags nodeFlags)
        {
            var size = TreeHelper.GetNodePageSize(kv, nodeFlags);
            var path = GetPagesForSplit(lltx, kv.Key, size);
            if (path.Count == 0)
            {
                throw new InvalidOperationException("internal error!");
            }

            return AddPsmtClusterEntry(lltx, path.Pop(), kv, path, nodeFlags);
        }

        protected DBOperationFlags AddPsmtClusterEntry(LowLevelTransaction lltx, TreePage page, KVTuple kv, Stack<TreePage> path, TreeNodeHeaderFlags nodeFlags)
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
                NodeFlags = nodeFlags,
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

            var ctx = SplitPage(lltx, page, path, kv.Key, matchIndex);
            if (ctx.Index >= matchIndex)
            {
                return AddPsmtClusterEntry(lltx, ctx.Current, kv, path, nodeFlags);
            }

            return AddPsmtClusterEntry(lltx, ctx.Sibling, kv, path, nodeFlags);
        }

        protected DBOperationFlags AddClusterOverflowEntry(LowLevelTransaction lltx, TreePage page, KVTuple kv)
        {
            return DBOperationFlags.Ok;
        }

        protected void AddSplitedBranchEntry(LowLevelTransaction lltx, TreePage page, long lPageNumber, long rPageNumber, Span<byte> key, Stack<TreePage> pages)
        {
            ref var header = ref page.TreeHeader;
            if (!header.NodeFlags.HasFlag(TreeNodeFlags.Branch))
            {
                throw new InvalidOperationException($"page:{page.Position} is not a branch page!");
            }

            if (header.Count == 0)
            {
                var ctx0 = new TreeNodeEntryAllocContext((ushort)key.Length, (ushort)key.Length, 0, TreeNodeHeaderFlags.Page);
                var ctx1 = new TreeNodeEntryAllocContext(0, 0, 0, TreeNodeHeaderFlags.Page);

                page.Alloc(lltx, 0, ref ctx0, out var entry1);
                page.Alloc(lltx, 1, ref ctx1, out var entry2);

                key.CopyTo(entry1.Key);
                entry1.Header.KeySize = (ushort)key.Length;
                entry1.Header.NodeFlags = TreeNodeHeaderFlags.Page;
                entry1.Header.PageNumber = lPageNumber;
                entry2.Header.PageNumber = rPageNumber;
                entry2.Header.NodeFlags = TreeNodeHeaderFlags.Page;

                //lltx.WriteFixedBTreeBranchPageInsertEntry(page.Position, key, lPageNumber, rPageNumber);
                return;
            }

            if (header.FreeSize < MaxBranchEntrySize)
            {
                var ctx = SplitBranch(lltx, page, pages, header.Count / 2);
                if (TreePage.CompareKey(key, ctx.Sibling.FirstKey) >= 0)
                {
                    AddSplitedBranchEntry(lltx, ctx.Sibling, lPageNumber, rPageNumber, key, pages);
                }
                else
                {
                    AddSplitedBranchEntry(lltx, ctx.Current, lPageNumber, rPageNumber, key, pages);
                }
            }
            else
            {
                var ctx = new TreeNodeEntryAllocContext()
                {
                    Key = key,
                    ValueSize = 0,
                    NodeFlags = TreeNodeHeaderFlags.Page,
                    KeySize = (ushort)key.Length,
                    Size = (ushort)key.Length
                };

                if (!page.AllocForKey(lltx, ref ctx, out _, out var index, out var entry))
                {
                    throw new InvalidOperationException("");
                }

                if (index == header.Count - 1)
                {
                    var prevEntry = page.GetNodeEntry(index - 1);

                    key.CopyTo(entry.Key);

                    entry.Header.KeySize = (ushort)key.Length;
                    entry.Header.PageNumber = lPageNumber;
                    entry.Header.NodeFlags = TreeNodeHeaderFlags.Page;
                    prevEntry.Header.PageNumber = rPageNumber;

                    page.SwitchNodeEntry(index - 1, index);
                }
                else
                {
                    var nextEntry = page.GetNodeEntry(index + 1);

                    key.CopyTo(entry.Key);

                    entry.Header.KeySize = (ushort)key.Length;
                    entry.Header.PageNumber = lPageNumber;
                    entry.Header.NodeFlags = TreeNodeHeaderFlags.Page;
                    nextEntry.Header.PageNumber = rPageNumber;
                }

                lltx.WriteBTreeBranchPageInsertEntry(page.Position, key, lPageNumber, rPageNumber);
            }
        }
    }
}