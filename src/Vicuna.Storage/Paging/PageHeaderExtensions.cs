using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Paging
{
    public static class PageHeaderExtensions
    {
        public static ref TTo Cast<TTo>(ref this PageHeader header) where TTo : struct
        {
            var sizeOf = Unsafe.SizeOf<TTo>();
            if (sizeOf != PageHeader.SizeOf)
            {
                throw new InvalidCastException($"the dest type:{typeof(TTo)} size has't a same size!");
            }

            return ref Unsafe.As<PageHeader, TTo>(ref header);
        }
    }
}
