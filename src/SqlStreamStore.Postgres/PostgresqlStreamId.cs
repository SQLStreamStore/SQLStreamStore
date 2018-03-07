namespace SqlStreamStore
{
    internal struct PostgresqlStreamId
    {
        internal readonly string Id;
        internal readonly string IdOriginal;

        public PostgresqlStreamId(string id, string idOriginal)
        {
            Id = id;
            IdOriginal = idOriginal;
        }

        public static readonly PostgresqlStreamId Deleted 
            = new StreamIdInfo(Streams.Deleted.DeletedStreamId).PostgresqlStreamId;
    }
}