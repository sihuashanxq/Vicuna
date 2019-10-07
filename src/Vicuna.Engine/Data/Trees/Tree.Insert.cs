using System;
using System.Collections.Generic;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Transactions;
using static Vicuna.Engine.Data.Trees.TreePage;

namespace Vicuna.Engine.Data.Trees
{
    public partial class Tree
    {
        public DBResult AddClusterEntry(LowLevelTransaction lltx, KVTuple kv, TreeNodeHeaderFlags nodeFlags)
        {
            var page = GetPageForUpdate(lltx, kv.Key, Constants.BTreeLeafPageDepth);
            if (page == null)
            {
                throw new NullReferenceException(nameof(page));
            }

            var dbResult = AddClusterEntry(lltx, page, kv, nodeFlags);
            if (dbResult.IsSplitPage() == false)
            {
                return dbResult;
            }

            var size = TreeHelper.GetNodeSizeInPage(kv, nodeFlags);
            var path = GetPagePathForKey(lltx, kv.Key, size);
            if (path.Count == 0)
            {
                throw new InvalidOperationException("internal error!");
            }

            return AddClusterEntry(lltx, path.Pop(), kv, path, nodeFlags);
        }

        protected DBResult AddClusterEntry(LowLevelTransaction lltx, TreePage page, KVTuple kv, TreeNodeHeaderFlags nodeFlags, bool isOpmst = true)
        {
            var matchFlags = 0;
            var matchIndex = 0;

            if (true)
            {
                page.SearchForAdd(lltx, kv.Key, out matchFlags, out matchIndex);
            }

            if (matchFlags == 0)
            {
                return AddClusterEntryInPlace(lltx, page, kv, matchIndex, nodeFlags);
            }

            var dbResult = LockRec(lltx, page, matchIndex, LockFlags.Document | LockFlags.Exclusive, true);
            if (dbResult.IsSuccess())
            {
                dbResult = AddKVToLeafPage(lltx, page, kv, matchIndex, nodeFlags);
            }

            if (isOpmst && dbResult.IsSplitPage())
            {
                lltx.ExitLatch(page.Position);
            }

            return dbResult;
        }

        protected DBResult AddClusterEntry(LowLevelTransaction lltx, TreePage page, KVTuple kv, Stack<TreePage> path, TreeNodeHeaderFlags nodeFlags)
        {
            var dbResult = AddClusterEntry(lltx, page, kv, nodeFlags, false);
            if (dbResult.IsSplitPage() == false)
            {
                return dbResult;
            }

            var ctx = SplitLeaf(lltx, page, path, kv.Key, page.LastMatchIndex);
            if (ctx.Current != page)
            {
                lltx.LockManager.SplitRecLock(page.Position, ctx.Sibling.Position, ctx.Index);
                lltx.LockManager.SplitRecLock(page.Position, ctx.Current.Position, 0);
            }
            else
            {
                lltx.LockManager.SplitRecLock(page.Position, ctx.Sibling.Position, ctx.Index);
            }

            if (ctx.Index >= page.LastMatchIndex)
            {
                return AddClusterEntry(lltx, ctx.Current, kv, nodeFlags);
            }
            else
            {
                return AddClusterEntry(lltx, ctx.Sibling, kv, nodeFlags);
            }
        }

        protected DBResult AddClusterEntryInPlace(LowLevelTransaction lltx, TreePage page, KVTuple kv, int index, TreeNodeHeaderFlags nodeFlags)
        {
            var e1 = page.GetNodeEntry(index);
            if (e1.Header.IsDeleted == false)
            {
                throw new InvalidOperationException("duplicate key for...");
            }

            var ctx = new TreeNodeEntryAllocContext()
            {
                Key = kv.Key,
                Size = (ushort)kv.Length,
                KeySize = (ushort)kv.Key.Length,
                ValueSize = (ushort)kv.Value.Length,
                NodeFlags = nodeFlags,
            };

            if (page.Alloc(lltx, index, ref ctx, out var entry))
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

                return DBResult.Success;
            }

            return DBResult.SplitPage;
        }

        protected void AddBranchPointerEntry(LowLevelTransaction lltx, TreePage page, long lPageNumber, long rPageNumber, Span<byte> key, Stack<TreePage> path)
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

                lltx.WriteBTreeBranchPageInsertEntry(page.Position, key, lPageNumber, rPageNumber);
                return;
            }

            if (header.FreeSize < MaxBranchEntrySize)
            {
                var ctx = SplitBranch(lltx, page, path, header.Count / 2);
                if (TreePage.CompareKey(key, ctx.Sibling.FirstKey) >= 0)
                {
                    AddBranchPointerEntry(lltx, ctx.Sibling, lPageNumber, rPageNumber, key, path);
                }
                else
                {
                    AddBranchPointerEntry(lltx, ctx.Current, lPageNumber, rPageNumber, key, path);
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

        protected DBResult AddKVToLeafPage(LowLevelTransaction lltx, TreePage page, KVTuple kv, int index, TreeNodeHeaderFlags nodeFlags)
        {
            var ctx = new TreeNodeEntryAllocContext()
            {
                Key = kv.Key,
                Size = (ushort)kv.Length,
                KeySize = (ushort)kv.Key.Length,
                ValueSize = (ushort)kv.Value.Length,
                NodeFlags = nodeFlags,
            };

            if (page.Alloc(lltx, index, ref ctx, out var entry))
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

                return DBResult.Success;
            }

            return DBResult.SplitPage;
        }
    }
}