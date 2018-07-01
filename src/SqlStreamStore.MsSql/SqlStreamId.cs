namespace SqlStreamStore
{
    internal struct SqlStreamId
    {
        internal readonly string Id;
        internal readonly string IdOriginal;

        public SqlStreamId(string id, string idOriginal)
        {
            Id = id;
            IdOriginal = idOriginal;
        }

        internal static readonly SqlStreamId Deleted = new StreamIdInfo(Streams.Deleted.DeletedStreamId).SqlStreamId;
    }
}