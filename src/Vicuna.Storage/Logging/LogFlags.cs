namespace Vicuna.Engine.Logging
{
    public enum LogFlags : byte
    {
        MLOG_BEGIN = 0,

        FILE_RAISE = 1,

        SET_BYTE_1 = 2,

        SET_BYTE_2 = 2,

        SET_BYTE_4 = 3,

        SET_BYTE_8 = 4,

        SET_BYTES = 5,

        BPAGE_LEAF_FREED = 6,

        BPAGE_LEAF_CREATED = 7,

        BPGE_LEAF_INSERT_ENTRY = 8,

        BPAGE_LEAF_DELTE_ENTRY = 9,

        BPAGE_BRANCH_FREED = 10,

        BPAGE_BRANCH_CREATED = 11,

        BPAGE_BRANCH_INSERT_ENTRY = 12,

        BPAGE_BRANCH_DELTE_ENTRY = 14,

        BPAGE_REORGANIZE = 15,

        FPAGE_FREED = 16,

        FPAGE_CREATED = 17,

        FPAGE_LEAF_INSERT_ENTRY = 18,

        FPAGE_LEAF_DELETE_ENTRY = 19,

        FPAGE_BRANCH_INSERT_ENTRY = 20,

        FPAGE_BRANCH_DELETE_ENTRY = 21,

        FPAGE_COPY_ENTRIES = 22,

        FPAGE_ROOT_SPLITTED = 23,

        MLOG_END = 255
    }
}
