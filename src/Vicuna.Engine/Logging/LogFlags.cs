namespace Vicuna.Engine.Logging
{
    public enum LogFlags : byte
    {
        MLOG_BEGIN = 0,

        FILE_RAISE = 1,

        SET_BYTE_1 = 2,

        SET_BYTE_2 = 3,

        SET_BYTE_4 = 4,

        SET_BYTE_8 = 5,

        SET_BYTES = 6,

        BPAGE_LEAF_INSERT_ENTRY = 7,

        BPAGE_LEAF_DELTE_ENTRY = 8,

        BPAGE_BRANCH_INSERT_ENTRY = 9,

        BPAGE_BRANCH_DELTE_ENTRY = 10,

        BPAGE_REORGANIZE = 11,

        BPAGE_COPY_ENTRIES = 12,

        BPAGE_Freed = 13,

        BPAGE_CREATED = 14,

        FBPAGE_FREED = 15,

        FBPAGE_CREATED = 16,

        FBPAGE_LEAF_INSERT_ENTRY = 17,

        FBPAGE_BRANCH_INSERT_ENTRY = 18,

        FBPAGE_DELETE_ENTRY = 19,

        FBPAGE_COPY_ENTRIES = 20,

        FBPAGE_ROOT_SPLITTED = 21,

        FBPAGE_ROOT_INITED = 22,

        MLOG_END = 255
    }
}
