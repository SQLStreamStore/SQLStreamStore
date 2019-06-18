namespace SqlStreamStore.V1
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

        internal static readonly SqlStreamId Deleted = new StreamIdInfo(V1.Streams.Deleted.DeletedStreamId).SqlStreamId;
    }
}