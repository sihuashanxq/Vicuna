namespace Vicuna.Engine.Data.Trees
{
    /// <summary>
    /// a mode to control the tree-page's query behavior
    /// </summary>
    public enum TreeNodeFetchMode
    {
        /// <summary>
        /// >
        /// </summary>
        MoreThan,

        /// <summary>
        /// >=
        /// </summary>
        MoreThanOrEqual,

        /// <summary>
        /// <
        /// </summary>
        LessThan,

        /// <summary>
        /// <=
        /// </summary>
        LessThanOrEqual
    }
}
