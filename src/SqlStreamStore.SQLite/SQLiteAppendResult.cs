namespace SqlStreamStore
{
    internal struct SQLiteAppendResult
    {
        public readonly int? MaxCount;
        public readonly int CurrentVersion;
        public readonly long CurrentPosition;

        public SQLiteAppendResult(int? maxCount, int currentVersion, long currentPosition)
        {
            MaxCount = maxCount;
            CurrentVersion = currentVersion;
            CurrentPosition = currentPosition;            
        }
    }
}