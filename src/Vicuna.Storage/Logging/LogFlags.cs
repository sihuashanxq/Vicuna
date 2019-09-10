namespace Vicuna.Engine.Logging
{
    public enum LogFlags : byte
    {
        LOG_BEGIN = 0,

        FILE_RAISE = 1,

        SET_BYTE_1 = 2,

        SET_BYTE_2 = 2,

        SET_BYTE_4 = 3,

        SET_BYTE_8 = 4,

        SET_BYTES = 5,

        BPAGE_LEAF_FREE = 6,

        BPAGE_LEAF_CREATE = 7,

        BPGE_LEAF_INSERT = 8,

        BPAGE_LEAF_DELTE = 9,

        BPAGE_BRANCH_FREE = 10,

        BPAGE_BRANCH_CREATE = 11,

        BPAGE_BRANCH_INSERT = 12,

        BPAGE_BRANCH_DELTE = 14,

        BPAGE_REORGANIZE = 15,

        FPAGE_LEAF_FREE = 16,

        FPAGE_LEAF_CREATE = 17,

        FPAGE_LEAF_INSERT = 18,

        FPAGE_LEAF_DELETE = 19,

        FPAGE_BRANCH_FREE = 20,

        FPAGE_BRANCH_CREATE = 21,

        FPAGE_BRANCH_INSERT = 22,

        FPAGE_BRANCH_DELETE = 23,

        LOG_END = 255
    }
}
