using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Paging
{
    public class Page
    {
        internal byte[] Data { get; }

        internal virtual int Size => Constants.PageSize;

        public ref PageHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref ReadAt<PageHeader>(0, PageHeader.SizeOf);
        }

        public ref PageFooter Footer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref ReadAt<PageFooter>(Size - PageFooter.SizeOf, PageFooter.SizeOf);
        }

        public ref PagePosition Position
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref ReadAt<PagePosition>(1, PagePosition.SizeOf);
        }

        public Page(byte[] buffer)
        {
            Data = buffer ?? throw new ArgumentNullException(nameof(buffer));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual Span<byte> ReadAt(int offset, int len)
        {
            if (offset < 0 || offset + len > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            return Data.AsSpan().Slice(offset, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ref T ReadAt<T>(int offset) where T : struct
        {
            return ref ReadAt<T>(offset, Unsafe.SizeOf<T>());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ref T ReadAt<T>(int offset, int sizeOf) where T : struct
        {
            if (offset < 0 || sizeOf + offset > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            return ref Unsafe.As<byte, T>(ref Data[offset]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void WriteTo<T>(int offset, T value) where T : struct
        {
            WriteTo(offset, value, Unsafe.SizeOf<T>());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void WriteTo<T>(int offset, T value, int sizeOf) where T : struct
        {
            if (offset < 0 || sizeOf + offset > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            Unsafe.As<byte, T>(ref Data[offset]) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void WriteTo(int offset, Span<byte> value)
        {
            if (offset < 0 || offset + value.Length > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            Unsafe.CopyBlockUnaligned(ref Data[offset], ref value[0], (uint)value.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void WriteTo(int offset, ref byte value, uint len)
        {
            if (offset < 0 || offset + len > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            Unsafe.CopyBlockUnaligned(ref Data[offset], ref value, len);
        }
    }
}
