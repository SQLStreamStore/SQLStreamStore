namespace Cedar.EventStore
{
    using System.Collections.Generic;

    public class StreamEventsPage
    {
        public readonly IReadOnlyCollection<StreamEvent> Events;
        public readonly int FromSequenceNumber;
        public readonly bool IsEndOfStream;
        public readonly int LastSequenceNumber;
        public readonly int NextSequenceNumber;
        public readonly ReadDirection ReadDirection;
        public readonly PageReadStatus Status;
        public readonly string StreamId;

        public StreamEventsPage(
            string streamId,
            PageReadStatus status,
            int fromSequenceNumber,
            int nextSequenceNumber,
            int lastSequenceNumber,
            ReadDirection direction,
            bool isEndOfStream,
            params StreamEvent[] events)
        {
            StreamId = streamId;
            Status = status;
            FromSequenceNumber = fromSequenceNumber;
            LastSequenceNumber = lastSequenceNumber;
            NextSequenceNumber = nextSequenceNumber;
            ReadDirection = direction;
            IsEndOfStream = isEndOfStream;
            Events = events;
        }
    }
}