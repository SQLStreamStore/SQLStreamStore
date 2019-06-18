namespace SqlStreamStore.V1
{
    using SqlStreamStore.V1.Imports.Ensure.That;

    internal struct StreamIdInfo // Love this name
    {
        public static readonly StreamIdInfo Deleted = new StreamIdInfo(V1.Streams.Deleted.DeletedStreamId);

        public readonly MySqlStreamId MySqlStreamId;

        public readonly MySqlStreamId MetadataMySqlStreamId;

        public StreamIdInfo(string idOriginal)
        {
            Ensure.That(idOriginal, nameof(idOriginal)).IsNotNullOrWhiteSpace();

            MySqlStreamId = new MySqlStreamId(idOriginal);
            MetadataMySqlStreamId = new MySqlStreamId("$$" + idOriginal);
        }
    }
}