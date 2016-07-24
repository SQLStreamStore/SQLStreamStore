namespace SqlStreamStore.Streams
{
    public sealed class AllMessagesPage
    {
        public readonly long FromCheckpoint;
        public readonly long NextCheckpoint;
        public readonly bool IsEnd;
        public readonly ReadDirection Direction;
        public readonly StreamMessage[] Messages;

        public AllMessagesPage(
            long fromCheckpoint,
            long nextCheckpoint,
            bool isEnd,
            ReadDirection direction,
            params StreamMessage[] messages)
        {
            FromCheckpoint = fromCheckpoint;
            NextCheckpoint = nextCheckpoint;
            IsEnd = isEnd;
            Direction = direction;
            Messages = messages;
        }

        public override string ToString()
        {
            return $"FromCheckpoint: {FromCheckpoint}, NextCheckpoint: {NextCheckpoint}, " +
                   $"IsEnd: {IsEnd}, Direction: {Direction}, SteamEventCount: {Messages.Length}";
        }
    }
}