using System;

namespace Vicuna.Engine.Extensions
{
    public unsafe static class ByteConverter
    {
        public static byte[] ToByteArray(this long[] args)
        {
            if (args == null)
            {
                throw new ArgumentNullException(nameof(args));
            }

            var buffer = new byte[args.Length * sizeof(long)];

            fixed (byte* ptr = buffer)
            {
                for (var i = 0; i < args.Length; i++)
                {
                    ((long*)ptr)[i] = args[i];
                }
            }

            return buffer;
        }
    }
}
