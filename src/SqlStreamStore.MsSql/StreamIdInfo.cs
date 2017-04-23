namespace SqlStreamStore
{
    using System;
    using System.Security.Cryptography;
    using System.Text;
    using SqlStreamStore.Imports.Ensure.That;

    internal struct StreamIdInfo // Hate this name
    {
        public readonly SqlStreamId SqlStreamId;

        public readonly SqlStreamId MetadataSqlStreamId;

        public StreamIdInfo(string idOriginal)
        {
            Ensure.That(idOriginal, "streamId").IsNotNullOrWhiteSpace();

            string id;
            Guid _;
            if(Guid.TryParse(idOriginal, out _))
            {
                id = idOriginal; //If the ID is a GUID, don't bother hashing it.
            }
            else
            {
                using(var sha1 = SHA1.Create())
                {
                    var hashBytes = sha1.ComputeHash(Encoding.UTF8.GetBytes(idOriginal));
                    id = BitConverter.ToString(hashBytes).Replace("-", string.Empty);
                }
            }
            SqlStreamId = new SqlStreamId(id, idOriginal);
            MetadataSqlStreamId = new SqlStreamId("$$" + id, "$$" + idOriginal);
        }
    }

    internal struct SqlStreamId
    {
        internal readonly string Id;
        internal readonly string IdOriginal;

        public SqlStreamId(string id, string idOriginal)
        {
            Id = id;
            IdOriginal = idOriginal;
        }

        internal static readonly SqlStreamId Deleted = new StreamIdInfo(SqlStreamStore.Deleted.DeletedStreamId).SqlStreamId;
    }
}