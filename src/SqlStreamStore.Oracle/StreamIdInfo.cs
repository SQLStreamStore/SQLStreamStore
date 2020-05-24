namespace SqlStreamStore.Oracle
{
    using SqlStreamStore.Imports.Ensure.That;

    internal struct StreamIdInfo // No feelings about this name
    {
        public readonly OracleStreamId StreamId;

        public readonly OracleStreamId MetaStreamId;

        public StreamIdInfo(string idOriginal)
        {
            Ensure.That(idOriginal, nameof(idOriginal)).IsNotNullOrWhiteSpace();

            StreamId = new OracleStreamId(idOriginal);
            MetaStreamId = new OracleStreamId("$$" + idOriginal);
        }
        
        public static StreamIdInfo Deleted = new StreamIdInfo(Streams.Deleted.DeletedStreamId);
    }
}