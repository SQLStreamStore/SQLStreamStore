namespace SqlStreamStore.V1.Streams
{
    /// <summary>
    ///     Constants for store position
    /// </summary>
    public static class Position
    {
        /// <summary>
        ///     No position
        /// </summary>
        public static readonly long? None = null;

        /// <summary>
        ///     The start of the store.
        /// </summary>
        public const long Start = 0;

        /// <summary>
        ///     The end of the store.
        /// </summary>
        public const long End = -1;
    }
}