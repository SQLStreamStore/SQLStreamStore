namespace SqlStreamStore.V1.Streams
{
    /// <summary>
    /// Constants for stream version
    /// </summary>
    public static class StreamVersion
    {
        /// <summary>
        ///     No stream version.
        /// </summary>
        public static readonly int? None = null;

        /// <summary>
        ///     The first message in a stream
        /// </summary>
        public const int Start = 0;

        /// <summary>
        ///     The last message in the stream.
        /// </summary>
        public const int End = -1;
    }
}