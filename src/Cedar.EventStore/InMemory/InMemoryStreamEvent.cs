namespace Cedar.EventStore.InMemory
{
    using System;

    internal class InMemoryStreamEvent
    {
        internal readonly long Checkpoint;
        internal readonly DateTimeOffset Created;
        internal readonly string StreamId;
        internal readonly Guid EventId;
        internal readonly string JsonData;
        internal readonly string JsonMetadata;
        internal readonly int StreamVersion;
        internal readonly string Type;

        internal InMemoryStreamEvent(
            string streamId,
            Guid eventId,
            int streamVersion,
            long checkpoint,
            DateTimeOffset created,
            string type,
            string jsonData,
            string jsonMetadata)
        {
            StreamId = streamId;
            EventId = eventId;
            StreamVersion = streamVersion;
            Checkpoint = checkpoint;
            Created = created;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata;
        }
    }
}