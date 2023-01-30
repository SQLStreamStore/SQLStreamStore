namespace SqlStreamStore.Streams
{
    public interface IReadAllPage
    {
        long FromPosition { get; }
        long NextPosition { get; }
        bool IsEnd { get; }
        ReadDirection Direction { get; }
        StreamMessage[] Messages { get; }
    }

    public class ReadAllPage : ReadAllPage<ReadAllPage>
    {
        public ReadAllPage(long fromPosition, long nextPosition, bool isEnd, ReadDirection direction, StreamMessage[] messages = null) : base(fromPosition, nextPosition, isEnd, direction, messages)
        { }
    }

    /// <summary>
    ///     Represents the result of a read of all streams.
    /// </summary>
    public abstract class ReadAllPage<TReadAllPage> : IReadAllPage where TReadAllPage : IReadAllPage
    {
        //private readonly ReadNextAllPage<TReadAllPage> _readNext;

        /// <summary>
        ///     A long representing the position where this page was read from.
        /// </summary>
        public long FromPosition { get; }

        /// <summary>
        ///     A long representing the position where the next page should be read from.
        /// </summary>
        public long NextPosition { get; }
   
        /// <summary>
        ///     True if page reach end of the all stream at time of reading. Otherwise false.
        /// </summary>
        public bool IsEnd { get; }

        /// <summary>
        ///     The direction of the the read request.
        /// </summary>
        public ReadDirection Direction { get; }

        /// <summary>
        ///     The collection of <see cref="StreamMessage"/>s returned as part of the read.
        /// </summary>
        public StreamMessage[] Messages { get; }

        /// <summary>
        ///     Initializes a new instance of <see cref="ReadAllPage"/>
        /// </summary>
        /// <param name="fromPosition">A long representing the position where this page was read from.</param>
        /// <param name="nextPosition">A long representing the position where the next page should be read from.</param>
        /// <param name="isEnd">True if page reach end of the all stream at time of reading. Otherwise false.</param>
        /// <param name="direction">The direction of the the read request.</param>
        /// <param name="messages">The collection messages read.</param>
        protected ReadAllPage(
            long fromPosition,
            long nextPosition,
            bool isEnd,
            ReadDirection direction,
            StreamMessage[] messages = null)
        {
            FromPosition = fromPosition;
            NextPosition = nextPosition;
            IsEnd = isEnd;
            Direction = direction;
            Messages = messages ?? new StreamMessage[0];
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"FromPosition: {FromPosition}, NextPosition: {NextPosition}, " +
                   $"IsEnd: {IsEnd}, Direction: {Direction}, SteamEventCount: {Messages.Length}";
        }

        ///// <summary>
        /////     Reads the next page.
        ///// </summary>
        ///// <param name="cancellationToken">A token to cancel the operations.</param>
        ///// <returns>A task the represents the asyncronous operation.</returns>
        //public Task<TReadAllPage> ReadNext(CancellationToken cancellationToken = default)
        //{
        //    return _readNext(NextPosition, cancellationToken);
        //}
    }
}