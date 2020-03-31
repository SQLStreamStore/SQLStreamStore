namespace SqlStreamStore
{
    using SqlStreamStore.Imports.Ensure.That;

    internal struct StreamIdInfo
    {
        public readonly SQLiteStreamId SQLiteStreamId;

        public readonly SQLiteStreamId MetadataSQLiteStreamId;

        public StreamIdInfo(string idOriginal)
        {
            Ensure.That(idOriginal, nameof(idOriginal)).IsNotNullOrWhiteSpace();

            SQLiteStreamId = new SQLiteStreamId(idOriginal);
            MetadataSQLiteStreamId = new SQLiteStreamId("$$" + idOriginal);
        }
    }
}