namespace SqlStreamStore.Streams
{
    public class AppendResult
    {
        public readonly int CurrentVersion;

        public AppendResult(int currentVersion)
        {
            CurrentVersion = currentVersion;
        }
    }
}