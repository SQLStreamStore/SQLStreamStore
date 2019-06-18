namespace SqlStreamStore.V1.Streams
{
    /// <summary>
    ///     Represents a streams metadata.
    /// </summary>
    public class MetadataMessage
    {
        /// <summary>
        /// The Stream Id.
        /// </summary>
        public string StreamId;

        /// <summary>
        /// The max age of messages retained in the stream.
        /// </summary>
        public int? MaxAge;

        /// <summary>
        /// The max count of message retained in the stream.
        /// </summary>
        public int? MaxCount;

        /// <summary>
        /// Custom Json 
        /// </summary>
        public string MetaJson;
    }
}