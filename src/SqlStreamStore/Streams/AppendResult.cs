namespace SqlStreamStore.Streams
{
    public class AppendResult
    {
        public readonly int CurrentVersion;
        public readonly long CurrentPosition;

        public AppendResult(int currentVersion)
        {
            CurrentVersion = currentVersion;
            CurrentPosition = -1;
        }

        public AppendResult(int currentVersion, long currentPosition)
        {
            CurrentVersion = currentVersion;
            CurrentPosition = currentPosition;
        }
    }
}