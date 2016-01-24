namespace Cedar.EventStore.Streams
{
    using System.Collections.Generic;

    public sealed class StreamEventsPage
    {
        public readonly IReadOnlyList<StreamEvent> Events;
        public readonly int FromStreamVersion;
        public readonly bool IsEndOfStream;
        public readonly int LastStreamVersion;
        public readonly int NextStreamVersion;
        public readonly ReadDirection ReadDirection;
        public readonly PageReadStatus Status;
        public readonly string StreamId;

        public StreamEventsPage(
            string streamId,
            PageReadStatus status,
            int fromStreamVersion,
            int nextStreamVersion,
            int lastStreamVersion,
            ReadDirection direction,
            bool isEndOfStream,
            params StreamEvent[] events)
        {
            StreamId = streamId;
            Status = status;
            FromStreamVersion = fromStreamVersion;
            LastStreamVersion = lastStreamVersion;
            NextStreamVersion = nextStreamVersion;
            ReadDirection = direction;
            IsEndOfStream = isEndOfStream;
            Events = events;
        }
    }
}