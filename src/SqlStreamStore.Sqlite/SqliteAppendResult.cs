namespace SqlStreamStore
{
    using SqlStreamStore.Streams;

    public class SqliteAppendResult : AppendResult
    {
        public int? MaxCount { get; }

        public SqliteAppendResult(int currentVersion, long currentPosition, int? maxCount) : base(currentVersion,
            currentPosition)
        {
            MaxCount = maxCount;
        }
    }
}