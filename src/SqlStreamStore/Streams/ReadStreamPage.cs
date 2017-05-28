namespace SqlStreamStore.Streams
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents the result of a read from a stream.
    /// </summary>
    public sealed class ReadStreamPage
    {
        /// <summary>
        ///     The collection of messages read.
        /// </summary>
        public readonly StreamMessage[] Messages;
        private readonly ReadNextStreamPage _readNext;

        /// <summary>
        ///     The version of the stream that read from.
        /// </summary>
        public readonly int FromStreamVersion;

        /// <summary>
        ///     Whether or not this is the end of the stream.
        /// </summary>
        public readonly bool IsEnd;

        /// <summary>
        ///     The version of the last message in the stream.
        /// </summary>
        public readonly int LastStreamVersion;

        /// <summary>
        ///     The position of the last message in the stream.
        /// </summary>
        public readonly long LastStreamPosition;

        /// <summary>
        ///     The next message version that can be read.
        /// </summary>
        public readonly int NextStreamVersion;

        /// <summary>
        ///     The direction of the read operation.
        /// </summary>
        public readonly ReadDirection ReadDirection;

        /// <summary>
        ///     The <see cref="PageReadStatus"/> of the read operation.
        /// </summary>
        public readonly PageReadStatus Status;

        /// <summary>
        ///     The id of the stream that was read.
        /// </summary>
        public readonly string StreamId;

        /// <summary>
        ///     Initialized a new instance of <see cref="ReadStreamPage"/>/
        /// </summary>
        /// <param name="streamId">The id of the stream that was read.</param>
        /// <param name="status">The <see cref="PageReadStatus"/> of the read operation.</param>
        /// <param name="fromStreamVersion">The version of the stream that read from.</param>
        /// <param name="nextStreamVersion">The next message version that can be read.</param>
        /// <param name="lastStreamVersion">The version of the last message in the stream.</param>
        /// <param name="lastStreamPosition">The position of the last message in the stream.</param>
        /// <param name="direction">The direction of the read operation.</param>
        /// <param name="isEnd">Whether or not this is the end of the stream.</param>
        /// <param name="readNext">An operation to read the next page.</param>
        /// <param name="messages">The messages read.</param>
        public ReadStreamPage(
            string streamId,
            PageReadStatus status,
            int fromStreamVersion,
            int nextStreamVersion,
            int lastStreamVersion,
            long lastStreamPosition,
            ReadDirection direction,
            bool isEnd, 
            ReadNextStreamPage readNext = null,
            StreamMessage[] messages = null)
        {
            StreamId = streamId;
            Status = status;
            FromStreamVersion = fromStreamVersion;
            LastStreamVersion = lastStreamVersion;
            LastStreamPosition = lastStreamPosition;
            NextStreamVersion = nextStreamVersion;
            ReadDirection = direction;
            IsEnd = isEnd;
            Messages = messages ?? new StreamMessage[0];
            _readNext = readNext ?? ((_, __) => throw new NotSupportedException());
        }

        public Task<ReadStreamPage> ReadNext(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _readNext(NextStreamVersion, cancellationToken);
        }
    }
}