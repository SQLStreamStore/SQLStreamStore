namespace SqlStreamStore.Streams
{
    public sealed class AllEventsPage
    {
        public readonly long FromCheckpoint;
        public readonly long NextCheckpoint;
        public readonly bool IsEnd;
        public readonly ReadDirection Direction;
        public readonly StreamMessage[] StreamMessages;

        public AllEventsPage(
            long fromCheckpoint,
            long nextCheckpoint,
            bool isEnd,
            ReadDirection direction,
            params StreamMessage[] streamMessages)
        {
            FromCheckpoint = fromCheckpoint;
            NextCheckpoint = nextCheckpoint;
            IsEnd = isEnd;
            Direction = direction;
            StreamMessages = streamMessages;
        }

        public override string ToString()
        {
            return $"FromCheckpoint: {FromCheckpoint}, NextCheckpoint: {NextCheckpoint}, " +
                   $"IsEnd: {IsEnd}, Direction: {Direction}, SteamEventCount: {StreamMessages.Length}";
        }
    }
}