using System;
using Vicuna.Storage.Abstractions.Stores;

namespace Vicuna.Storage.Abstractions.Stores
{
    public abstract class Store : IStore
    {
        public abstract int Id { get; }

        public abstract long Length { get; }

        public abstract object SyncRoot { get; }

        public abstract void Sync();

        public abstract void SetLength(long length);

        protected abstract void Dispose(bool disposing);

        public virtual void Read(long pos, Span<byte> buffer)
        {
            lock (SyncRoot)
            {
                if (pos < 0 || buffer.Length < 0 || pos + buffer.Length > Length)
                {
                    throw new ArgumentOutOfRangeException($"read {buffer.Length} bytes at pos: {pos} out of the store's size!");
                }

                InternalRead(pos, buffer);
            }
        }

        public virtual void Write(long pos, Span<byte> buffer)
        {
            lock (SyncRoot)
            {
                if (pos < 0 || buffer.Length < 0 || pos + buffer.Length > Length)
                {
                    throw new ArgumentOutOfRangeException($"read {buffer.Length} bytes at pos: {pos} out of the store's size!");
                }

                InternalWrite(pos, buffer);
            }
        }

        public virtual void Write(long pos, byte[] buffer, int offset, int len)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            if (offset < 0 || len < 0 || offset + len > buffer.Length)
            {
                throw new ArgumentOutOfRangeException($"buffer length:{buffer.Length},offset:{offset},len:{len}");
            }

            Write(pos, buffer.AsSpan().Slice(offset, len));
        }

        protected abstract void InternalRead(long pos, Span<byte> buffer);

        protected abstract void InternalWrite(long pos, Span<byte> buffer);

        ~Store()
        {
            Dispose(true);
        }

        public virtual void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
