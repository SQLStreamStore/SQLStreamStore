namespace Cedar.EventStore
{
    using System.Collections.Generic;

    public class StreamEventsPage
    {
        public readonly IReadOnlyCollection<StreamEvent> Events;
        public readonly int FromStreamRevision;
        public readonly bool IsEndOfStream;
        public readonly int LastStreamRevision;
        public readonly int NextStreamRevision;
        public readonly ReadDirection ReadDirection;
        public readonly PageReadStatus Status;
        public readonly string StreamId;

        public StreamEventsPage(
            string streamId,
            PageReadStatus status,
            int fromStreamRevision,
            int nextStreamRevision,
            int lastStreamRevision,
            ReadDirection direction,
            bool isEndOfStream,
            params StreamEvent[] events)
        {
            StreamId = streamId;
            Status = status;
            FromStreamRevision = fromStreamRevision;
            LastStreamRevision = lastStreamRevision;
            NextStreamRevision = nextStreamRevision;
            ReadDirection = direction;
            IsEndOfStream = isEndOfStream;
            Events = events;
        }
    }
}