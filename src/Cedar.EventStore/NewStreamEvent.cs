namespace Cedar.EventStore
{
    using System;
    using EnsureThat;

    public sealed class NewStreamEvent
    {
        public readonly object Data;
        public readonly Guid EventId;
        public readonly byte[] Metadata;

        public NewStreamEvent(Guid eventId, object data, byte[] metadata = null) // TODO metadata dictionary
        {
            Ensure.That(eventId, "eventId").IsNotEmpty();
            Ensure.That(data, "data").IsNotNull();

            EventId = eventId;
            Data = data;
            Metadata = metadata ?? new byte[0];
        }
    }
}