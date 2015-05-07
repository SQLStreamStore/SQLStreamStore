namespace Cedar.EventStore
{
    using System;

    public class StreamEvent
    {
        public readonly string Checkpoint;
        public readonly DateTime Created;
        public readonly Guid EventId;
        public readonly string JsonData;
        public readonly string JsonMetadata;
        public readonly int SequenceNumber;
        public readonly string StreamId;
        public readonly string Type;

        public StreamEvent(
            string streamId,
            Guid eventId,
            int sequenceNumber,
            string checkpoint,
            DateTime created,
            string type,
            string jsonData,
            string jsonMetadata)
        {
            EventId = eventId;
            StreamId = streamId;
            SequenceNumber = sequenceNumber;
            Checkpoint = checkpoint;
            Created = created;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata;
        }
    }
}