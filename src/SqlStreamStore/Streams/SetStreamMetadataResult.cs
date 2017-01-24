namespace SqlStreamStore.Streams
{
    public class SetStreamMetadataResult
    {
        public readonly int CurrentVersion;

        public SetStreamMetadataResult(int currentVersion)
        {
            CurrentVersion = currentVersion;
        }
    }
}