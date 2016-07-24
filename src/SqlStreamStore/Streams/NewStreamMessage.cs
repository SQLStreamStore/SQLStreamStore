namespace SqlStreamStore.Streams
{
    using System;
    using EnsureThat;

    public struct NewStreamMessage
    {
        public readonly string JsonData;
        public readonly Guid EventId;
        public readonly string Type;
        public readonly string JsonMetadata;

        public NewStreamMessage(Guid eventId, string type, string jsonData, string jsonMetadata = null)
        {
            Ensure.That(eventId, "eventId").IsNotEmpty();
            Ensure.That(type, "type").IsNotNullOrEmpty();
            Ensure.That(jsonData, "data").IsNotNullOrEmpty();

            EventId = eventId;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = jsonMetadata ?? string.Empty;
        }
    }
}