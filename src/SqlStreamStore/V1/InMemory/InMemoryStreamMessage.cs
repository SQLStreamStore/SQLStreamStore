namespace SqlStreamStore.V1.InMemory
{
    using System;

    internal sealed class InMemoryStreamMessage
    {
        internal readonly long Position;
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
            long position,
            DateTime created,
            string type,
            string jsonData,
            string jsonMetadata)
        {
            StreamId = streamId;
            MessageId = messageId;
            StreamVersion = streamVersion;
            Position = position;
            Created = created;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata;
        }
    }
}