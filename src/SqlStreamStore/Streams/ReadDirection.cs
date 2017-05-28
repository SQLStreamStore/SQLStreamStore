namespace SqlStreamStore.Streams
{
    /// <summary>
    /// Represents the direction of read operation.
    /// </summary>
    public enum ReadDirection
    {
        /// <summary>
        ///     From the start to the end.
        /// </summary>
        Forward,

        /// <summary>
        ///     From the end to the start.
        /// </summary>
        Backward
    }
}