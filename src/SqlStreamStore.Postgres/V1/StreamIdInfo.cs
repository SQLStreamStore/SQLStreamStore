namespace SqlStreamStore.V1
{
    using SqlStreamStore.V1.Imports.Ensure.That;

    internal struct StreamIdInfo // Love this name
    {
        public readonly PostgresqlStreamId PostgresqlStreamId;

        public readonly PostgresqlStreamId MetadataPosgresqlStreamId;

        public StreamIdInfo(string idOriginal)
        {
            Ensure.That(idOriginal, nameof(idOriginal)).IsNotNullOrWhiteSpace();

            PostgresqlStreamId = new PostgresqlStreamId(idOriginal);
            MetadataPosgresqlStreamId = new PostgresqlStreamId("$$" + idOriginal);
        }
    }
}