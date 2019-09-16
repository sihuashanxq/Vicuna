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

        BPAGE_LEAF_FREED = 7,

        BPAGE_LEAF_CREATED = 8,

        BPAGE_LEAF_INSERT_ENTRY = 9,

        BPAGE_LEAF_DELTE_ENTRY = 10,

        BPAGE_BRANCH_FREED = 11,

        BPAGE_BRANCH_CREATED = 12,

        BPAGE_BRANCH_INSERT_ENTRY = 13,

        BPAGE_BRANCH_DELTE_ENTRY = 14,

        BPAGE_REORGANIZE = 15,

        FBPAGE_FREED = 16,

        FBPAGE_CREATED = 17,

        FBPAGE_LEAF_INSERT_ENTRY = 18,

        FBPAGE_BRANCH_INSERT_ENTRY = 19,

        FBPAGE_DELETE_ENTRY = 20,

        FBPAGE_COPY_ENTRIES = 21,

        FBPAGE_ROOT_SPLITTED = 22,

        FBPAGE_ROOT_INITED = 23,

        MLOG_END = 255
    }
}
