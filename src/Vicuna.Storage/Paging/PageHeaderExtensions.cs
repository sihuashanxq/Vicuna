using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Paging
{
    public static class PageHeaderExtensions
    {
        public static ref TTo Cast<TTo>(ref this PageHeader header) where TTo : struct
        {
            return ref Unsafe.As<PageHeader, TTo>(ref header);
        }
    }
}
