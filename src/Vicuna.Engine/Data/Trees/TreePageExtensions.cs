﻿using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Data.Trees
{
    public static class TreePageExtensions
    {
        public static TreePage AsTree(this Page page, TreeNodeQueryMode mode = TreeNodeQueryMode.Lte)
        {
            if (page == null)
            {
                return null;
            }

            return new TreePage(page.Data, mode);
        }
    }
}