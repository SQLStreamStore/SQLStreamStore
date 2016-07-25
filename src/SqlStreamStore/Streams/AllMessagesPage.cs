namespace SqlStreamStore.Streams
{
    /// <summary>
    ///     Represents the result of a read of all streams.
    /// </summary>
    public sealed class AllMessagesPage
    {
        public readonly long FromPosition;
        public readonly long NextPosition;
        public readonly bool IsEnd;
        public readonly ReadDirection Direction;
        public readonly StreamMessage[] Messages;

        public AllMessagesPage(
            long fromPosition,
            long nextPosition,
            bool isEnd,
            ReadDirection direction,
            params StreamMessage[] messages)
        {
            FromPosition = fromPosition;
            NextPosition = nextPosition;
            IsEnd = isEnd;
            Direction = direction;
            Messages = messages;
        }

        public override string ToString()
        {
            return $"FromPosition: {FromPosition}, NextPosition: {NextPosition}, " +
                   $"IsEnd: {IsEnd}, Direction: {Direction}, SteamEventCount: {Messages.Length}";
        }
    }
}