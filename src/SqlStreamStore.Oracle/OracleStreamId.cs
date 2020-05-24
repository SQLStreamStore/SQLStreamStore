namespace SqlStreamStore
{
    using System;
    using System.Security.Cryptography;
    using System.Text;

    internal struct OracleStreamId : IEquatable<OracleStreamId>
    {
        public readonly string Id;
        public readonly string IdOriginal;

        public OracleStreamId(string idOriginal)
        {
            Id = Guid.TryParse(idOriginal, out _)
                ? idOriginal
                : ComputeHash(idOriginal);
            IdOriginal = idOriginal;
        }

        private static string ComputeHash(string idOriginal)
        {
            using(var sha1 = SHA1.Create())
            {
                var hashBytes = sha1.ComputeHash(Encoding.UTF8.GetBytes(idOriginal));
                return BitConverter.ToString(hashBytes).Replace("-", string.Empty);
            }
        }

       

        public static bool operator ==(OracleStreamId left, OracleStreamId right) => left.Equals(right);
        public static bool operator !=(OracleStreamId left, OracleStreamId right) => !left.Equals(right);

        public bool Equals(OracleStreamId other)
            => string.Equals(Id, other.Id) && string.Equals(IdOriginal, other.IdOriginal);

        public override bool Equals(object obj) => obj is OracleStreamId other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return (Id.GetHashCode() * 397) ^ IdOriginal.GetHashCode();
            }
        }

        public override string ToString() => IdOriginal;
    }
}