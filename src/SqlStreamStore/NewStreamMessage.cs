namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Imports.Ensure.That;

    public class NewStreamMessage
    {
        public readonly string JsonData;
        public readonly Guid MessageId;
        public readonly string Type;
        public readonly string JsonMetadata;

        public NewStreamMessage(Guid messageId, string type, string jsonData, string jsonMetadata = null)
        {
            Ensure.That(messageId, "MessageId").IsNotEmpty();
            Ensure.That(type, "type").IsNotNullOrEmpty();
            Ensure.That(jsonData, "data").IsNotNullOrEmpty();

            MessageId = messageId;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata ?? string.Empty;
        }
    }
}