namespace Cedar.EventStore.Streams
{
    public class StreamMetadataResult
    {
        public readonly string StreamId;
        public readonly int MetadataStreamVersion;
        public readonly int? MaxAge;
        public readonly int? MaxCount;
        public readonly string MetadataJson;

        public StreamMetadataResult(
            string streamId,
            int metadataStreamVersion,
            int? maxAge = null,
            int? maxCount = null,
            string metadataJson = null)
        {
            StreamId = streamId;
            MetadataStreamVersion = metadataStreamVersion;
            MaxAge = maxAge;
            MaxCount = maxCount;
            MetadataJson = metadataJson;
        }
    }
}