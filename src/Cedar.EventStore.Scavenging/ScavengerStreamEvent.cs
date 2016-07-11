namespace Cedar.EventStore.Scavenging
{
    using System;

    public class ScavengerStreamEvent
    {
        public ScavengerStreamEvent(string streamId, Guid eventId, DateTime created)
        {
            StreamId = streamId;
            EventId = eventId;
            Created = created;
        }

        public string StreamId { get; }

        public Guid EventId { get; }

        public DateTime Created { get; }

        public DateTime Expires { get; private set; } = DateTime.MaxValue;

        internal void SetExpires(int? maxAge)
        {
            Expires = maxAge == null ? DateTime.MaxValue : Created.AddSeconds(maxAge.Value);
        }
    }
}