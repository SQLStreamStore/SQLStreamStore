namespace SqlStreamStore.Streams
{
    /// <summary>
    ///     Stream expected version constants.
    /// </summary>
    public static class ExpectedVersion
    {
        /// <summary>
        ///     Any version.
        /// </summary>
        public const int Any = -2;

        /// <summary>
        ///     Stream should not exist. If stream exists, then will be considered a concurrency problem.
        /// </summary>
        public const int NoStream = -1;    }
}
