namespace SqlStreamStore
{
    internal struct SqliteStreamId
    {
        internal readonly string Id;
        internal readonly string IdOriginal;

        public SqliteStreamId(string id, string idOriginal)
        {
            Id = id;
            IdOriginal = idOriginal;
        }

        internal static readonly SqliteStreamId Deleted = new StreamIdInfo(Streams.Deleted.DeletedStreamId).SqlStreamId;
    }
}