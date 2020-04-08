namespace SqlStreamStore
{
    using SqlStreamStore.Streams;

    public class SqliteAppendResult : AppendResult
    {
        public int InternalStreamId { get; }

        public SqliteAppendResult(int currentVersion, long currentPosition, int internalStreamId) : base(currentVersion,
            currentPosition)
        {
            InternalStreamId = internalStreamId;
        }
    }
}