namespace SqlStreamStore.Streams
{
    /// <summary>
    ///     Represents the status of a page read.
    /// </summary>
    public enum PageReadStatus
    {
        /// <summary>
        ///     The stream was successfully read.
        /// </summary>
        Success,

        /// <summary>
        ///     The stream was not found.
        /// </summary>
        StreamNotFound,
    }
}