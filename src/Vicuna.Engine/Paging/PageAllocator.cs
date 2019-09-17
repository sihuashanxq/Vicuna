using System.Collections.Generic;
using Vicuna.Engine.Data.Trees.Fixed;
using Vicuna.Engine.Paging;
using Vicuna.Engine.Transactions;

public abstract class PageAllocator
{
    public abstract void Free(LowLevelTransaction lltx, PagePosition page);

    public abstract PagePosition[] Allocate(LowLevelTransaction lltx, int fileId, int count);
}