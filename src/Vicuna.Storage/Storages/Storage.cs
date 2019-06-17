using System;

namespace Vicuna.Engine.Storages
{
    public class Storage
    {
        public virtual int Id { get; }

        public virtual object SyncRoot { get; }

        public virtual StorageFile File { get; }

        public virtual long Length => File.Length;

        public Storage(int id, StorageFile file)
        {
            Id = id;
            File = file;
            SyncRoot = new object();
        }

        public virtual void Sync()
        {
            lock (SyncRoot)
            {
                File.Flush(true);
            }
        }

        public void Truncate(long length)
        {
            lock (SyncRoot)
            {
                if (length > Length)
                {
                    AddLength(Length - length);
                }
                else
                {
                    File.SetLength(length);
                    File.Flush(true);
                }
            }
        }

        public long AddLength(long len)
        {
            lock (SyncRoot)
            {
                if (len < Constants.MB * 16)
                {
                    len = GetFileRaiseLength(len);
                }

                var begin = Length;
                var index = Length;
                var buffer = new byte[Math.Min(len, Constants.MB)];
                var count = len % Constants.MB == 0 ? len / Constants.MB : len / Constants.MB + 1;
                var newLength = begin + len;

                File.SetLength(newLength);

                for (var i = 0; i < count; i++)
                {
                    if (i == count - 1)
                    {
                        File.Write(index, buffer, 0, (int)(newLength - index));
                        continue;
                    }

                    File.Write(index, buffer);
                    index += buffer.Length;
                }

                File.Flush(true);

                return newLength;
            }
        }

        public void Read(long pos, Span<byte> buffer)
        {
            lock (SyncRoot)
            {
                File.Read(pos, buffer);
            }
        }

        public void Write(long pos, Span<byte> buffer)
        {
            lock (SyncRoot)
            {
                File.Write(pos, buffer);
            }
        }

        public void Write(long pos, byte[] buffer, int offset, int len)
        {
            lock (SyncRoot)
            {
                File.Write(pos, buffer, offset, len);
            }
        }

        ~Storage()
        {
            Dispose(true);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Dispose(bool disposing)
        {
            File.Dispose();
        }

        private long GetFileRaiseLength(long minLength)
        {
            if (Length > Constants.MB * 256)
            {
                return Math.Max(minLength, Constants.MB * 16);
            }

            if (Length > Constants.MB * 128)
            {
                return Math.Max(minLength, Constants.MB * 8);
            }

            if (Length > Constants.MB * 64)
            {
                return Math.Max(minLength, Constants.MB * 4);
            }

            if (Length > Constants.MB * 32)
            {
                return Math.Max(minLength, Constants.MB * 2);
            }

            if (Length > Constants.MB * 16)
            {
                return Math.Max(minLength, Constants.MB);
            }

            if (Length > Constants.MB * 8)
            {
                return Math.Max(minLength, Constants.KB * 512);
            }

            if (Length > Constants.MB)
            {
                return Math.Max(minLength, Constants.KB * 256);
            }

            return Math.Max(minLength, Constants.KB * 64);
        }
    }
}
