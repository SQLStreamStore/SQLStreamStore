namespace Cedar.EventStore.Streams
{
    public sealed class AllEventsPage
    {
        public readonly long FromCheckpoint;
        public readonly long NextCheckpoint;
        public readonly bool IsEnd;
        public readonly ReadDirection Direction;
        public readonly StreamEvent[] StreamEvents;

        public AllEventsPage(
            long fromCheckpoint,
            long nextCheckpoint,
            bool isEnd,
            ReadDirection direction,
            params StreamEvent[] streamEvents)
        {
            FromCheckpoint = fromCheckpoint;
            NextCheckpoint = nextCheckpoint;
            IsEnd = isEnd;
            Direction = direction;
            StreamEvents = streamEvents;
        }

        public override string ToString()
        {
            return $"FromCheckpoint: {FromCheckpoint}, NextCheckpoint: {NextCheckpoint}, " +
                   $"IsEnd: {IsEnd}, Direction: {Direction}, SteamEventCount: {StreamEvents.Length}";
        }
    }
}