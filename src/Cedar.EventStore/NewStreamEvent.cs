namespace Cedar.EventStore
{
    using System;
    using EnsureThat;

    public sealed class NewStreamEvent
    {
        public readonly string JsonData;
        public readonly Guid EventId;
        public readonly string JsonMetadata;

        public NewStreamEvent(Guid eventId, string jsonData, string metadata = null)
        {
            Ensure.That(eventId, "eventId").IsNotEmpty();
            Ensure.That(jsonData, "data").IsNotNull();

            EventId = eventId;
            JsonData = jsonData;
            JsonMetadata = metadata;
        }
    }
}