namespace SqlStreamStore
{
    using System;
    using System.Security.Cryptography;
    using System.Text;

    internal struct MySqlStreamId : IEquatable<MySqlStreamId>
    {
        public readonly string Id;
        public readonly string IdOriginal;

        public MySqlStreamId(string idOriginal)
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

        public static readonly MySqlStreamId Deleted
            = new MySqlStreamId(Streams.Deleted.DeletedStreamId);

        public static bool operator ==(MySqlStreamId left, MySqlStreamId right) => left.Equals(right);
        public static bool operator !=(MySqlStreamId left, MySqlStreamId right) => !left.Equals(right);

        public bool Equals(MySqlStreamId other)
            => string.Equals(Id, other.Id) && string.Equals(IdOriginal, other.IdOriginal);

        public override bool Equals(object obj) => obj is MySqlStreamId other && Equals(other);

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