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
        /// the storage's id which is page in
        /// </summary>
        [FieldOffset(0)]
        public int StorageId;

        /// <summary>
        /// the page's number in storage
        /// </summary>
        [FieldOffset(4)]
        public long PageNumber;

        public PagePosition(int storageId, long pageNumber)
        {
            StorageId = storageId;
            PageNumber = pageNumber;
        }

        public override int GetHashCode()
        {
            var hashCode = StorageId;

            hashCode += hashCode * 31 ^ PageNumber.GetHashCode();

            return hashCode;
        }

        public override bool Equals(object obj)
        {
            if (base.Equals(obj))
            {
                return true;
            }

            if (obj is PagePosition pos)
            {
                return pos.StorageId == StorageId && pos.PageNumber == PageNumber;
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
            return $"{StorageId}:{PageNumber}";
        }
    }
}
