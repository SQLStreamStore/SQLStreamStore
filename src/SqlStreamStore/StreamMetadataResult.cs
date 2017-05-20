namespace SqlStreamStore
{
    public class StreamMetadataResult
    {
        /// <summary>
        ///     The stream ID.
        /// </summary>
        public readonly string StreamId;
        /// <summary>
        ///     The verson of the metadata stream. Can be used for concurrency control 
        /// </summary>
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