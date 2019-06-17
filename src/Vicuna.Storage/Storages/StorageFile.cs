using System;
using System.IO;

namespace Vicuna.Engine.Storages
{
    public class StorageFile
    {
        private bool _disposed;

        private readonly FileStream _fileStream;

        public virtual long Length
        {
            get => _fileStream.Length;
        }

        protected internal virtual Stream Stream
        {
            get => _fileStream;
        }

        public StorageFile(FileStream fileStream)
        {
            _fileStream = fileStream ?? throw new ArgumentNullException(nameof(fileStream));
        }

        public virtual void Flush(bool flushToDisk)
        {
            CheckDisposed();
            _fileStream.Flush(flushToDisk);
        }

        public virtual void SetLength(long length)
        {
            CheckDisposed();
            Stream.SetLength(length);
        }

        public virtual void Read(long pos, Span<byte> buffer)
        {
            if (pos < 0 || buffer.Length < 0 || pos + buffer.Length > Length)
            {
                throw new ArgumentOutOfRangeException($"read {buffer.Length} bytes at pos: {pos} out of the store's size!");
            }

            CheckDisposed();
            InternalRead(pos, buffer);
        }

        public virtual void Write(long pos, Span<byte> buffer)
        {
            if (pos < 0 || buffer.Length < 0 || pos + buffer.Length > Length)
            {
                throw new ArgumentOutOfRangeException($"read {buffer.Length} bytes at pos: {pos} out of the store's size!");
            }

            CheckDisposed();
            InternalWrite(pos, buffer);
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

            CheckDisposed();
            Write(pos, buffer.AsSpan().Slice(offset, len));
        }

        /// <summary>
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="buffer"></param>
        protected virtual void InternalRead(long pos, Span<byte> buffer)
        {
            Stream.Seek(pos, SeekOrigin.Begin);
            Stream.Read(buffer);
        }

        /// <summary>
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="buffer"></param>
        protected virtual void InternalWrite(long pos, Span<byte> buffer)
        {
            CheckDisposed();
            Stream.Seek(pos, SeekOrigin.Begin);
            Stream.Write(buffer);
        }

        ~StorageFile()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// </summary>
        /// <param name="disposing"></param>
        protected void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            lock (this)
            {
                if (_disposed)
                {
                    return;
                }

                _fileStream.Flush(true);
                _fileStream.Dispose();
                _disposed = true;
            }
        }

        /// <summary>
        /// check whether the store was disposed
        /// </summary>
        protected virtual void CheckDisposed()
        {
            if (_disposed)
            {
                throw new InvalidOperationException($"");
            }
        }
    }
}
