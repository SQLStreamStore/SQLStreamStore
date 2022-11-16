namespace SqlStreamStore
{
    using System;
    using System.Security.Cryptography;
    using System.Text;

    internal struct PostgresqlStreamId : IEquatable<PostgresqlStreamId>
    {
        public readonly string Id;
        public readonly string IdOriginal;

        public PostgresqlStreamId(string idOriginal)
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
                return Convert.ToHexString(hashBytes);
            }
        }

        public static readonly PostgresqlStreamId Deleted
            = new PostgresqlStreamId(Streams.Deleted.DeletedStreamId);

        public static bool operator ==(PostgresqlStreamId left, PostgresqlStreamId right) => left.Equals(right);
        public static bool operator !=(PostgresqlStreamId left, PostgresqlStreamId right) => !left.Equals(right);

        public bool Equals(PostgresqlStreamId other)
            => string.Equals(Id, other.Id) && string.Equals(IdOriginal, other.IdOriginal);

        public override bool Equals(object obj) => obj is PostgresqlStreamId other && Equals(other);

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