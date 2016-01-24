namespace Cedar.EventStore.InMemory
{
    using System;

    internal class InMemoryStreamEvent
    {
        internal readonly long Checkpoint;
        public readonly DateTimeOffset Created;
        public readonly Guid EventId;
        public readonly string JsonData;
        public readonly string JsonMetadata;
        public readonly int StreamVersion;
        public readonly string Type;

        internal InMemoryStreamEvent(
            Guid eventId,
            int streamVersion,
            long checkpoint,
            DateTimeOffset created,
            string type,
            string jsonData,
            string jsonMetadata)
        {
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