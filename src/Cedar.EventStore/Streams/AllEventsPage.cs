namespace Cedar.EventStore.Streams
{
    using System.Collections.Generic;

    public sealed class AllEventsPage
    {
        public readonly long FromCheckpoint;
        public readonly long NextCheckpoint;
        public readonly bool IsEnd;
        public readonly ReadDirection Direction;
        public readonly IReadOnlyCollection<StreamEvent> StreamEvents;

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
    }
}