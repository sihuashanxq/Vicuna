using System;
using System.IO;
using Vicuna.Engine.Locking;
using Vicuna.Engine.Paging;

namespace Vicuna.Engine.Storages
{
    public class File
    {
        private bool _disposed;

        public int Id { get; }

        public FileStream Stream { get; }

        public LatchEntry Latch { get; }

        public PagePosition Root { get; }

        public string Name => Stream.Name;

        public long Length => Stream.Length;

        public File(int id, FileStream stream)
        {
            Id = id;
            Root = new PagePosition(id, 0);
            Latch = new LatchEntry(this);
            Stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public virtual void Flush(bool flushToDisk = false)
        {
            lock (this)
            {
                Stream.Flush(flushToDisk);
            }
        }

        public void Truncate(long length)
        {
            lock (this)
            {
                if (length > Length)
                {
                    AddLength(Length - length);
                }
                else
                {
                    Stream.SetLength(length);
                    Stream.Flush(true);
                }
            }
        }

        public long AddLength(long len)
        {
            lock (this)
            {
                if (len < Constants.MB * 16)
                {
                    len = GetPreRaiseLength(len);
                }

                var begin = Length;
                var index = Length;
                var buffer = new byte[Math.Min(len, Constants.MB)];
                var count = len % Constants.MB == 0 ? len / Constants.MB : len / Constants.MB + 1;
                var newLength = begin + len;

                Stream.SetLength(newLength);

                for (var i = 0; i < count; i++)
                {
                    if (i == count - 1)
                    {
                        Write(index, buffer, 0, (int)(newLength - index));
                        continue;
                    }

                    Write(index, buffer);
                    index += buffer.Length;
                }

                Flush(true);
                return newLength;
            }
        }

        public void Read(long pos, Span<byte> buffer)
        {
            lock (this)
            {
                if (pos < 0 || buffer.Length < 0 || pos + buffer.Length > Length)
                {
                    throw new ArgumentOutOfRangeException($"read {buffer.Length} bytes at pos: {pos} out of the store's size!");
                }

                InternalRead(pos, buffer);
            }
        }

        public void Write(long pos, Span<byte> buffer)
        {
            lock (this)
            {
                if (pos < 0 || buffer.Length < 0 || pos + buffer.Length > Length)
                {
                    throw new ArgumentOutOfRangeException($"read {buffer.Length} bytes at pos: {pos} out of the store's size!");
                }

                InternalWrite(pos, buffer);
            }
        }

        public void Write(long pos, byte[] buffer, int offset, int len)
        {
            lock (this)
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
            Stream.Seek(pos, SeekOrigin.Begin);
            Stream.Write(buffer);
        }

        /// <summary>
        /// </summary>
        /// <param name="min"></param>
        /// <returns></returns>
        private long GetPreRaiseLength(long min)
        {
            if (Length > Constants.MB * 256)
            {
                return Math.Max(min, Constants.MB * 16);
            }

            if (Length > Constants.MB * 128)
            {
                return Math.Max(min, Constants.MB * 8);
            }

            if (Length > Constants.MB * 64)
            {
                return Math.Max(min, Constants.MB * 4);
            }

            if (Length > Constants.MB * 32)
            {
                return Math.Max(min, Constants.MB * 2);
            }

            if (Length > Constants.MB * 16)
            {
                return Math.Max(min, Constants.MB);
            }

            if (Length > Constants.MB * 8)
            {
                return Math.Max(min, Constants.KB * 512);
            }

            if (Length > Constants.MB)
            {
                return Math.Max(min, Constants.KB * 256);
            }

            return Math.Max(min, Constants.KB * 64);
        }

        ~File()
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
            lock (this)
            {
                if (!_disposed)
                {
                    _disposed = true;
                    Flush(true);
                    Stream.Dispose();
                }
            }
        }
    }
}
