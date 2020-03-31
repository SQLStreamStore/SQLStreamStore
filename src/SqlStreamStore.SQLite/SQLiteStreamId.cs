namespace SqlStreamStore
{
    using System;
    using System.Security.Cryptography;
    using System.Text;

    internal struct SQLiteStreamId : IEquatable<SQLiteStreamId>
    {
        public readonly string Id;
        public readonly string IdOriginal;

        public SQLiteStreamId(string idOriginal)
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

        public static readonly SQLiteStreamId Deleted = new SQLiteStreamId(Streams.Deleted.DeletedStreamId);
        public static bool operator ==(SQLiteStreamId left, SQLiteStreamId right) => left.Equals(right);
        public static bool operator !=(SQLiteStreamId left, SQLiteStreamId right) => !left.Equals(right);

        public bool Equals(SQLiteStreamId other)
            => string.Equals(Id, other.Id) && string.Equals(IdOriginal, other.IdOriginal);

        public override bool Equals(object obj) => obj is SQLiteStreamId other && Equals(other);

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