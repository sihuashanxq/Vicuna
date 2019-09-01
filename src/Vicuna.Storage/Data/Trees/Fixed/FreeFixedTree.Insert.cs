using System;
using Vicuna.Engine.Transactions;

namespace Vicuna.Engine.Data.Trees.Fixed
{
    public partial class FreeFixedTree
    {
        public DBOperationFlags AddEntry(LowLevelTransaction lltx, long key, Span<byte> value)
        {
            var page = GetPageForUpdate(lltx, key, -1);
            if (page.LastMatch == 0)
            {
                return DBOperationFlags.Ok;
            }

            return AddEntry(lltx, page, key, value);
        }

        protected DBOperationFlags AddEntry(LowLevelTransaction lltx, FreeFixedTreePage page, long pageNumber, Span<byte> value)
        {
            if (!page.Alloc(page.LastMatchIndex, out var entry))
            {
                SplitLeafPage(lltx, page, pageNumber, page.FixedHeader.Count / 2);
                return DBOperationFlags.Ok;
            }

            if (page.FixedHeader.DataElementSize < value.Length)
            {
                throw new InvalidOperationException($@"the value-size:{value.Length} of node 
                        must be lessThan or equals:{page.FixedHeader.DataElementSize} bytes 
                        in page:{page.Position}");
            }

            entry.Header.PageNumber = pageNumber;
            value.CopyTo(entry.Value);

            return DBOperationFlags.Ok;
        }
    }
}
