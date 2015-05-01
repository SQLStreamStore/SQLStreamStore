using System;

namespace Cedar.EventStore
{
    public sealed class NewStreamEvent
    {
        public readonly Guid EventId;
        public readonly byte[] Body;
        public readonly byte[] Metadata;

        public NewStreamEvent(Guid eventId, byte[] body, byte[] metadata = null)
        {
            EventId = eventId;
            Body = body ?? new byte[0];
            Metadata = metadata ?? new byte[0];
        }
    }
}