namespace SqlStreamStore.Streams
{
    /// <summary>
    /// Constants for stream version
    /// 
    /// </summary>
    public static class StreamVersion
    {
        /// <summary>
        /// The first event in a stream
        /// 
        /// </summary>
        public const int Start = 0;
        /// <summary>
        /// The last event in the stream.
        /// 
        /// </summary>
        public const int End = -1;
    }
}