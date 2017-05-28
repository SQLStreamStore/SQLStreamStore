namespace SqlStreamStore.Streams
{

    /// <summary>
    ///     Represents the result of a steam metadata read.
    /// </summary>
    public class StreamMetadataResult
    {
        /// <summary>
        ///     The stream ID.
        /// </summary>
        public readonly string StreamId;
        
        /// <summary>
        ///     The verson of the metadata stream. Can be used for concurrency control.
        /// </summary>
        public readonly int MetadataStreamVersion;

        /// <summary>
        ///     The max age of messages in the stream.
        /// </summary>
        public readonly int? MaxAge;

        /// <summary>
        ///     The max count of message in the stream.
        /// </summary>
        public readonly int? MaxCount;

        /// <summary>
        ///     Custom metadata serialized as JSON.
        /// </summary>
        public readonly string MetadataJson;

        /// <summary>
        ///     Initialized a new instance of <see cref="StreamMetadataResult"/>.
        /// </summary>
        /// <param name="streamId">The stream ID.</param>
        /// <param name="metadataStreamVersion">The verson of the metadata stream.</param>
        /// <param name="maxAge">The max age of messages in the stream.</param>
        /// <param name="maxCount">The max count of message in the stream.</param>
        /// <param name="metadataJson">Custom metadata serialized as JSON.</param>
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