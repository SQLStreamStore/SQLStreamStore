namespace SqlStreamStore.Streams
{
    using System;

    public struct StreamMessage
    {
        public readonly long Position;
        public readonly DateTime CreatedUtc;
        public readonly Guid MessageId;
        public readonly string JsonData;
        public readonly string JsonMetadata;
        public readonly int StreamVersion;
        public readonly string StreamId;
        public readonly string Type;

        public StreamMessage(
            string streamId,
            Guid messageId,
            int streamVersion,
            long position,
            DateTime createdUtc,
            string type,
            string jsonData,
            string jsonMetadata)
        {
            MessageId = messageId;
            StreamId = streamId;
            StreamVersion = streamVersion;
            Position = position;
            CreatedUtc = createdUtc;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata;
        }

        public override string ToString()
        {
            return $"MessageId={MessageId} StreamId={StreamId} StreamVersion={StreamVersion} Position={Position} Type={Type}";
        }
    }
}