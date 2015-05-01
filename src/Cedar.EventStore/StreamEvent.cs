using System;
using System.Collections.Generic;

namespace Cedar.EventStore
{
    public sealed class StreamEvent
    {
        public readonly Guid EventId;
        public readonly string StreamId;
        public readonly int SequenceNumber;
        public readonly string Checkpoint;
        public readonly IReadOnlyCollection<byte> Body;
        public readonly IReadOnlyCollection<byte> Metadata;

        public StreamEvent(
            string streamId,
            Guid eventId,
            int sequenceNumber,
            string checkpoint,
            IReadOnlyCollection<byte> body,
            IReadOnlyCollection<byte> metadata)
        {
            EventId = eventId;
            StreamId = streamId;
            SequenceNumber = sequenceNumber;
            Checkpoint = checkpoint;
            Body = body;
            Metadata = metadata;
        }
    }
}