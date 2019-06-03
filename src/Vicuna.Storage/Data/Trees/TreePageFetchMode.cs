namespace Vicuna.Storage.Data.Trees
{
    /// <summary>
    /// a mode to control the tree-page's query behavior
    /// </summary>
    public enum TreePageFetchMode
    {
        /// <summary>
        /// =
        /// </summary>
        Equal,

        /// <summary>
        /// >
        /// </summary>
        MoreThan,

        /// <summary>
        /// <
        /// </summary>
        LessThan,

        /// <summary>
        /// >=
        /// </summary>
        MoreThanOrEqual,

        /// <summary>
        /// <=
        /// </summary>
        LessThanOrEqual
    }
}
