using System.Runtime.CompilerServices;

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsError(this DBResult dbResult)
        {
            return dbResult == DBResult.Error;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSuccess(this DBResult dbResult)
        {
            return dbResult == DBResult.Success;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsWaitLock(this DBResult dbResult)
        {
            return dbResult == DBResult.WaitLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsDeadlock(this DBResult dbResult)
        {
            return dbResult == DBResult.DeadLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSplitPage(this DBResult dbResult)
        {
            return dbResult == DBResult.SplitPage;
        }
    }
}