namespace SqlStreamStore
{
    internal struct MySqlStreamId
    {
        internal readonly string Id;
        internal readonly string IdOriginal;

        public MySqlStreamId(string id, string idOriginal)
        {
            Id = id;
            IdOriginal = idOriginal;
        }

        internal static readonly MySqlStreamId Deleted = new StreamIdInfo(Streams.Deleted.DeletedStreamId).SqlStreamId;
    }
}