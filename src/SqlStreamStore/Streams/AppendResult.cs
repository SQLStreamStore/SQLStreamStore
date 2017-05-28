namespace SqlStreamStore.Streams
{
    /// <summary>
    ///     Represents the result of an append to stream operation.
    /// </summary>
    public class AppendResult
    {
        /// <summary>
        ///     The current version the stream that was affected by the append operation is at.
        /// </summary>
        public readonly int CurrentVersion;

        /// <summary>
        ///     The current position the stream that was affected by the append operation is at.
        /// </summary>
        public readonly long CurrentPosition;

        /// <summary>
        ///     Initializes a new instance of <see cref="AppendResult"/>
        /// </summary>
        /// <param name="currentVersion">The current version the stream that was affected by the append operation is at.</param>
        /// <param name="currentPosition">The current position the stream that was affected by the append operation is at.</param>
        public AppendResult(int currentVersion, long currentPosition)
        {
            CurrentVersion = currentVersion;
            CurrentPosition = currentPosition;
        }
    }
}