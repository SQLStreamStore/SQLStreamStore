namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Streams;

    internal class HttpStreamMessage
    {
        public Guid MessageId { get; set; }
        public DateTimeOffset CreatedUtc { get; set; }
        public long Position { get; set; }
        public string StreamId { get; set; }
        public int StreamVersion { get; set; }
        public string Type { get; set; }
        public string Payload { get; set; }
        
        public static implicit operator StreamMessage(HttpStreamMessage message)
            => new StreamMessage(
                message.StreamId,
                message.MessageId,
                message.StreamVersion,
                message.Position,
                message.CreatedUtc.DateTime,
                message.Type,
                null,
                message.Payload);
    }
}