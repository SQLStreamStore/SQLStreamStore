namespace Cedar.EventStore.Scavenging
{
    public class ScavengerStreamMetadata
    {
        public readonly string StreamId;
        public readonly int? MaxAge;
        public readonly int? MaxCount;

        public ScavengerStreamMetadata(string streamId, int? maxAge, int? maxCount)
        {
            StreamId = streamId;
            MaxAge = maxAge;
            MaxCount = maxCount;
        }
    }
}