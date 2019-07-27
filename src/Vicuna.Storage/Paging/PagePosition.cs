using System.Runtime.InteropServices;

namespace Vicuna.Engine.Paging
{
    /// <summary>
    /// a structure describing the location of page
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = SizeOf)]
    public struct PagePosition
    {
        public const int SizeOf = sizeof(int) + sizeof(long);

        /// <summary>
        /// the file's id which is page in
        /// </summary>
        [FieldOffset(0)]
        public int FileId;

        /// <summary>
        /// the page's number in storage
        /// </summary>
        [FieldOffset(4)]
        public long PageNumber;

        public PagePosition(int fileId, long pageNumber)
        {
            FileId = fileId;
            PageNumber = pageNumber;
        }

        public override int GetHashCode()
        {
            return FileId + (FileId * 31 ^ PageNumber.GetHashCode());
        }

        public override bool Equals(object obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (obj is PagePosition pos)
            {
                return FileId == pos.FileId && PageNumber == pos.PageNumber;
            }

            return false;
        }

        public static bool operator ==(PagePosition p1, PagePosition p2)
        {
            return p1.Equals(p2);
        }

        public static bool operator !=(PagePosition p1, PagePosition p2)
        {
            return !p1.Equals(p2);
        }

        public override string ToString()
        {
            return $"{FileId}:{PageNumber}";
        }
    }
}
