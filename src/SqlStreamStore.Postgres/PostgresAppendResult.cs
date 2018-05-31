namespace SqlStreamStore
{
    internal struct PostgresAppendResult
    {
        public readonly int? MaxCount;
        public readonly int CurrentVersion;
        public readonly long CurrentPosition;

        public PostgresAppendResult(int? maxCount, int currentVersion, long currentPosition)
        {
            MaxCount = maxCount;
            CurrentVersion = currentVersion;
            CurrentPosition = currentPosition;
        }
    }
}