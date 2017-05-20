namespace SqlStreamStore
{
    using SqlStreamStore.Streams;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents the result of a read of all streams.
    /// </summary>
    public sealed class ReadAllPage
    {
        public readonly long FromPosition;
        public readonly long NextPosition;
        public readonly bool IsEnd;
        public readonly ReadDirection Direction;
        private readonly ReadNextAllPage _readNext;
        public readonly StreamMessage[] Messages;

        public ReadAllPage(
            long fromPosition,
            long nextPosition,
            bool isEnd,
            ReadDirection direction,
            StreamMessage[] messages,
            ReadNextAllPage readNext)
        {
            FromPosition = fromPosition;
            NextPosition = nextPosition;
            IsEnd = isEnd;
            Direction = direction;
            Messages = messages;
            _readNext = readNext;
        }

        public override string ToString()
        {
            return $"FromPosition: {FromPosition}, NextPosition: {NextPosition}, " +
                   $"IsEnd: {IsEnd}, Direction: {Direction}, SteamEventCount: {Messages.Length}";
        }

        public Task<ReadAllPage> ReadNext(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _readNext(NextPosition, cancellationToken);
        }
    }
}