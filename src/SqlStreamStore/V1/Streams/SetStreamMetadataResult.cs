namespace SqlStreamStore.V1.Streams
{
    /// <summary>
    ///     Represents the result of setting a stream's metadata.
    /// </summary>
    public class SetStreamMetadataResult
    {
        /// <summary>
        ///     The current version of the stream at the time
        ///     the metadata was written.
        /// </summary>
        public readonly int CurrentVersion;

        /// <summary>
        ///     Initializes a new instance of the <see cref="SetStreamMetadataResult"/>.
        /// </summary>
        /// <param name="currentVersion"></param>
        public SetStreamMetadataResult(int currentVersion)
        {
            CurrentVersion = currentVersion;
        }
    }
}