using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Engine.Paging
{
    public class Page
    {
        internal byte[] Data;

        internal ref byte Ptr => ref Data[0];

        internal virtual int Size => Constants.PageSize;

        public ref PageHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref Read<PageHeader>(0);
        }

        public ref PageTailer Tailer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref Read<PageTailer>(Size - 1 - PageTailer.SizeOf, PageTailer.SizeOf);
        }

        public ref PagePosition Position
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref Read<PagePosition>(1);
        }

        public Page(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (data.Length != Size)
            {
                throw new ArgumentOutOfRangeException($" data size not equal page size!");
            }

            Data = data;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual Span<byte> Slice(int offset, int len)
        {
            if (offset < 0 || offset + len > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            return Data.AsSpan().Slice(offset, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ref T Read<T>(int offset) where T : struct
        {
            return ref Read<T>(offset, Unsafe.SizeOf<T>());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ref T Read<T>(int offset, int sizeOf) where T : struct
        {
            if (offset < 0 || sizeOf + offset > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            return ref Unsafe.As<byte, T>(ref Data[offset]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void Write<T>(int offset, T value) where T : struct
        {
            Write(offset, value, Unsafe.SizeOf<T>());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void Write<T>(int offset, T value, int sizeOf) where T : struct
        {
            if (offset < 0 || sizeOf + offset > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            Unsafe.As<byte, T>(ref Data[offset]) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void Write(int offset, Span<byte> value)
        {
            if (offset < 0 || offset + value.Length > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            Unsafe.CopyBlockUnaligned(ref Data[offset], ref value[0], (uint)value.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void Write(int offset, ref byte value, uint len)
        {
            if (offset < 0 || offset + len > Size)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            Unsafe.CopyBlockUnaligned(ref Data[offset], ref value, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(Page dest)
        {
            Unsafe.CopyBlockUnaligned(ref dest.Data[0], ref Data[0], (uint)Data.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page CreateCopy()
        {
            var buffer = new byte[Data.Length];

            Unsafe.CopyBlockUnaligned(ref buffer[0], ref Data[0], (uint)buffer.Length);

            return new Page(buffer);
        }
    }
}
