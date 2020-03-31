namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Streams;

    internal class SQLiteNewStreamMessage
    {
        public Guid MessageId { get; set; }
        public string JsonData { get; set; }
        public string JsonMetadata { get; set; }
        public string Type { get; set; }

        public static SQLiteNewStreamMessage FromNewStreamMessage(NewStreamMessage message)
            => new SQLiteNewStreamMessage
            {
                MessageId = message.MessageId,
                Type = message.Type,
                JsonData = message.JsonData,
                JsonMetadata = string.IsNullOrEmpty(message.JsonMetadata) ? null : message.JsonMetadata,
            };
    }
}