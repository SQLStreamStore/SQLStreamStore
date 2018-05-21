namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Security.Cryptography;

    // Adapted from https://github.com/LogosBible/Logos.Utility/blob/master/src/Logos.Utility/GuidUtility.cs
    // MIT Licence

    /// <summary>
    ///     A helper utility to generate deterministed GUIDS.
    /// </summary>
    public class DeterministicGuidGenerator
    {
        private readonly byte[] _namespaceBytes;

        /// <summary>
        ///     Initializes a new instance of <see cref="DeterministicGuidGenerator"/>
        /// </summary>
        /// <param name="guidNameSpace">
        ///     A namespace that ensures that the GUID generated with this instance
        ///     do not collided with other generators. Your application should define
        ///     it's namespace as a constant.
        /// </param>
        public DeterministicGuidGenerator(Guid guidNameSpace)
        {
            _namespaceBytes = guidNameSpace.ToByteArray();
            SwapByteOrder(_namespaceBytes);
        }

        /// <summary>
        ///     Creates a deterministic GUID.
        /// </summary>
        /// <param name="source">
        ///     A source to generate the GUID from.
        /// </param>
        /// <returns>
        ///     A deterministically generated GUID.
        /// </returns>
        public Guid Create(byte[] source)
        {
            byte[] hash;
            using (var algorithm = SHA1.Create())
            {
                algorithm.TransformBlock(_namespaceBytes, 0, _namespaceBytes.Length, null, 0);
                algorithm.TransformFinalBlock(source, 0, source.Length);

                hash = algorithm.Hash;
            }

            var newGuid = new byte[16];
            Array.Copy(hash, 0, newGuid, 0, 16);

            newGuid[6] = (byte)((newGuid[6] & 0x0F) | (5 << 4));
            newGuid[8] = (byte)((newGuid[8] & 0x3F) | 0x80);

            SwapByteOrder(newGuid);
            return new Guid(newGuid);
        }

        private static void SwapByteOrder(byte[] guid)
        {
            SwapBytes(guid, 0, 3);
            SwapBytes(guid, 1, 2);
            SwapBytes(guid, 4, 5);
            SwapBytes(guid, 6, 7);
        }

        private static void SwapBytes(byte[] guid, int left, int right)
        {
            var temp = guid[left];
            guid[left] = guid[right];
            guid[right] = temp;
        }
    }
}
