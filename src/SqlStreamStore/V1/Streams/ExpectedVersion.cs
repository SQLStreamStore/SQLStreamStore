namespace SqlStreamStore.V1.Streams
{
    /// <summary>
    ///     Stream expected version constants.
    /// </summary>
    public static class ExpectedVersion
    {
        /// <summary>
        ///     Stream should exist but be empty. If stream does not exist, or
        ///     contains messages, then will be considered a concurrency problem.
        /// </summary>
        public const int EmptyStream = -1;

        /// <summary>
        ///     Any version.
        /// </summary>
        public const int Any = -2;

        /// <summary>
        ///     Stream should not exist. If stream exists, then will be considered
        ///     a concurrency problem.
        /// </summary>
        public const int NoStream = -3;
    }
}
