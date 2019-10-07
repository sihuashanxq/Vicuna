namespace Vicuna.Engine
{
    public enum DBResult
    {
        None = 0,

        Error,

        Success,

        WaitLock,

        DeadLock,

        SplitPage
    }

    public static class DBResultExtensions
    {
        public static bool IsError(this DBResult dbResult)
        {
            return dbResult == DBResult.Error;
        }

        public static bool IsSuccess(this DBResult dbResult)
        {
            return dbResult == DBResult.Success;
        }

        public static bool IsWaitLock(this DBResult dbResult)
        {
            return dbResult == DBResult.WaitLock;
        }

        public static bool IsDeadlock(this DBResult dbResult)
        {
            return dbResult == DBResult.DeadLock;
        }

        public static bool IsSplitPage(this DBResult dbResult)
        {
            return dbResult == DBResult.SplitPage;
        }
    }
}