using System;

namespace Vicuna.Storage.Stores
{
    public interface IStore
    {
        int Id { get; }

        long Length { get; }

        void Sync();

        void SetLength(long length);

        int Read(long pos, Span<byte> buffer);

        int Read(long pos, byte[] buffer, int offset, int count);

        void Write(long pos, Span<byte> buffer);

        void Write(long pos, byte[] buffer, int offset, int count);
    }
}
