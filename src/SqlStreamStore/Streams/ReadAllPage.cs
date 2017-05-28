namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents the result of a read of all streams.
    /// </summary>
    public sealed class ReadAllPage
    {
        /// <summary>
        ///     A long representing the position where this page was read from.
        /// </summary>
        public readonly long FromPosition;

        /// <summary>
        ///     A long representing the position where the next page should be read from.
        /// </summary>
        public readonly long NextPosition;
   
        /// <summary>
        ///     True if page reach end of the all stream at time of reading. Otherwise false.
        /// </summary>
        public readonly bool IsEnd;

        /// <summary>
        ///     The direction of the the read request.
        /// </summary>
        public readonly ReadDirection Direction;
        private readonly ReadNextAllPage _readNext;

        /// <summary>
        ///     The collection of <see cref="StreamMessage"/>s returned as part of the read.
        /// </summary>
        public readonly StreamMessage[] Messages;

        /// <summary>
        ///     Initializes a new instance of <see cref="ReadAllPage"/>
        /// </summary>
        /// <param name="fromPosition">A long representing the position where this page was read from.</param>
        /// <param name="nextPosition">A long representing the position where the next page should be read from.</param>
        /// <param name="isEnd">True if page reach end of the all stream at time of reading. Otherwise false.</param>
        /// <param name="direction">The direction of the the read request.</param>
        /// <param name="readNext">An operation to read the next page of messages.</param>
        /// <param name="messages">The collection messages read.</param>
        public ReadAllPage(
            long fromPosition,
            long nextPosition,
            bool isEnd,
            ReadDirection direction,
            ReadNextAllPage readNext,
            StreamMessage[] messages = null)
        {
            FromPosition = fromPosition;
            NextPosition = nextPosition;
            IsEnd = isEnd;
            Direction = direction;
            _readNext = readNext;
            Messages = messages ?? new StreamMessage[0];
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"FromPosition: {FromPosition}, NextPosition: {NextPosition}, " +
                   $"IsEnd: {IsEnd}, Direction: {Direction}, SteamEventCount: {Messages.Length}";
        }

        /// <summary>
        ///     Reads the next page.
        /// </summary>
        /// <param name="cancellationToken">A token to cancel the operations.</param>
        /// <returns>A task the represents the asyncronous operation.</returns>
        public Task<ReadAllPage> ReadNext(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _readNext(NextPosition, cancellationToken);
        }
    }
}