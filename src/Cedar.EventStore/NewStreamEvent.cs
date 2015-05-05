using System;

namespace Cedar.EventStore
{
    using EnsureThat;

    public sealed class NewStreamEvent
    {
        public readonly Guid EventId;
        public readonly object Data;
        public readonly byte[] Metadata;

        public NewStreamEvent(Guid eventId, object data, byte[] metadata = null)
        {
            Ensure.That(eventId, "eventId").IsNotEmpty();
            Ensure.That(data, "data").IsNotNull();

            EventId = eventId;
            Data = data;
            Metadata = metadata ?? new byte[0];
        }
    }
}