namespace Cedar.EventStore
{
    using System;

    public class StreamEvent
    {
        public readonly string Checkpoint;
        public readonly string Type;
        public readonly string JsonData;
        public readonly Guid EventId;
        public readonly string JsonMetadata;
        public readonly int SequenceNumber;
        public readonly string StreamId;

        public StreamEvent(
            string streamId,
            Guid eventId,
            int sequenceNumber,
            string checkpoint,
            string type,
            string jsonData,
            string jsonMetadata)
        {
            EventId = eventId;
            StreamId = streamId;
            SequenceNumber = sequenceNumber;
            Checkpoint = checkpoint;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata;
        }
    }
}