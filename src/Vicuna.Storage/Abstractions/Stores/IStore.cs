using System;

namespace Vicuna.Storage.Abstractions.Stores
{
    public interface IStore : IDisposable
    {
        #region properties

        /// <summary>
        /// </summary>
        int Id { get; }

        /// <summary>
        /// </summary>
        long Length { get; }

        /// <summary>
        /// </summary>
        object SyncRoot { get; }

        #endregion

        #region methods

        /// <summary>
        /// </summary>
        void Sync();

        /// <summary>
        /// </summary>
        /// <param name="length"></param>
        void SetLength(long length);

        /// <summary>
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="data"></param>
        void Read(long pos, Span<byte> data);

        /// <summary>
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="data"></param>
        void Write(long pos, Span<byte> data);

        #endregion
    }
}
