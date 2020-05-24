namespace SqlStreamStore
{
    using System;
    using System.Security.Cryptography;
    using System.Text;
    using SqlStreamStore.Imports.Ensure.That;

    internal struct StreamIdInfo // Hate this name
    {
        public readonly SqliteStreamId SqlStreamId;

        public readonly SqliteStreamId MetadataSqlStreamId;

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
            SqlStreamId = new SqliteStreamId(id, idOriginal);
            MetadataSqlStreamId = new SqliteStreamId("$$" + id, "$$" + idOriginal);
        }
    }
}