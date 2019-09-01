using System;
using System.Collections.Generic;
using System.Text;

namespace Vicuna.Engine.Paging.Free
{
    public class FreeListPage : Page
    {
        public FreeListPage(byte[] data) : base(data)
        {

        }

        public void AddNodeEntry(int fileId, long pageNumber, ushort size)
        {

        }
    }
}
