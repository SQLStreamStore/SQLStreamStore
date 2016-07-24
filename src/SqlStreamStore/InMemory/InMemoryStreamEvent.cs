namespace SqlStreamStore.InMemory
{
    using System;

    internal sealed class InMemoryStreamEvent
    {
        internal readonly long Checkpoint;
        internal readonly DateTime Created;
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
            DateTime created,
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