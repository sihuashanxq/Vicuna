using System;
using System.IO;
using Vicuna.Storage.Abstractions.Stores;

namespace Vicuna.Storage.Stores
{
    /// <summary>
    /// </summary>
    public class FileDataStore : Store
    {
        private bool _disposed;

        /// <summary>
        /// the store's persistent file
        /// </summary>
        public FileStream File { get; }

        /// <summary>
        /// store's Id
        /// </summary>
        public override int Id { get; }

        /// <summary>
        /// store's length
        /// </summary>
        public override long Length => File.Length;

        /// <summary>
        /// gets an object that can be used to synchronize access to the store
        /// </summary>
        public override object SyncRoot => this;

        /// <summary>
        /// </summary>
        /// <param name="id"></param>
        /// <param name="file"></param>
        public FileDataStore(int id, FileStream file)
        {
            Id = id;
            File = file ?? throw new ArgumentNullException(nameof(file));
        }

        /// <summary>
        /// </summary>
        /// <param name="length"></param>
        public override void SetLength(long length)
        {
            lock (SyncRoot)
            {
                CheckDisposed();
                File.SetLength(length);
            }
        }

        /// <summary>
        /// </summary>
        public override void Sync()
        {
            lock (SyncRoot)
            {
                CheckDisposed();
                File.Flush(true);
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="buffer"></param>
        protected override void InternalRead(long pos, Span<byte> buffer)
        {
            CheckDisposed();
            File.Seek(pos, SeekOrigin.Begin);
            File.Read(buffer);
        }

        /// <summary>
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="buffer"></param>
        protected override void InternalWrite(long pos, Span<byte> buffer)
        {
            CheckDisposed();
            File.Seek(pos, SeekOrigin.Begin);
            File.Write(buffer);
        }

        /// <summary>
        /// </summary>
        /// <param name="disposing"></param>
        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            lock (SyncRoot)
            {
                if (!_disposed)
                {
                    Sync();
                    _disposed = true;
                }
            }
        }

        /// <summary>
        /// check whether the store was disposed
        /// </summary>
        protected virtual void CheckDisposed()
        {
            if (_disposed)
            {
                throw new InvalidOperationException($"store id:{Id} was disposed!");
            }
        }
    }
}