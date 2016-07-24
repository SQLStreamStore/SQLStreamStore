namespace SqlStreamStore.InMemory
{
    using System;

    internal sealed class InMemoryStreamMessage
    {
        internal readonly long Checkpoint;
        internal readonly DateTime Created;
        internal readonly string StreamId;
        internal readonly Guid MessageId;
        internal readonly string JsonData;
        internal readonly string JsonMetadata;
        internal readonly int StreamVersion;
        internal readonly string Type;

        internal InMemoryStreamMessage(
            string streamId,
            Guid messageId,
            int streamVersion,
            long checkpoint,
            DateTime created,
            string type,
            string jsonData,
            string jsonMetadata)
        {
            StreamId = streamId;
            MessageId = messageId;
            StreamVersion = streamVersion;
            Checkpoint = checkpoint;
            Created = created;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata;
        }
    }
}