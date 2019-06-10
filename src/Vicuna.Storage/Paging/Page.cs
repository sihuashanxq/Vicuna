using System;
using System.Runtime.CompilerServices;

namespace Vicuna.Storage.Paging
{
    public class Page
    {
        internal byte[] Data;

        internal ref byte Ptr => ref Data[0];

        internal virtual int Size => Constants.PageSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref THeader GetHeader<THeader>(int sizeOf) where THeader : struct, IPageHeader
        {
            return ref Read<THeader>(0, sizeOf);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref TTailer GetTailer<TTailer>(int sizeOf) where TTailer : struct, IPageTailer
        {
            return ref Read<TTailer>(Size - 1 - sizeOf, sizeOf);
        }

        internal PagePosition Position
        {
            get
            {
                ref var header = ref GetHeader<PageHeader>(PageHeader.SizeOf);
                return new PagePosition(header.StoreId, header.PageNumber);
            }
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
            if (offset + len > Size)
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
    }
}
