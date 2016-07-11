namespace Cedar.EventStore.Streams
{
    using System;

    public struct StreamEvent
    {
        public readonly long Checkpoint;
        public readonly DateTime Created;
        public readonly Guid EventId;
        public readonly string JsonData;
        public readonly string JsonMetadata;
        public readonly int StreamVersion;
        public readonly string StreamId;
        public readonly string Type;

        public StreamEvent(
            string streamId,
            Guid eventId,
            int streamVersion,
            long checkpoint,
            DateTime created,
            string type,
            string jsonData,
            string jsonMetadata)
        {
            EventId = eventId;
            StreamId = streamId;
            StreamVersion = streamVersion;
            Checkpoint = checkpoint;
            Created = created;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata;
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString()
        {
            return $"EventId={EventId} StreamId={StreamId} StreamVersion={StreamVersion} Checkpoint={Checkpoint} Type={Type}";
        }
    }
}