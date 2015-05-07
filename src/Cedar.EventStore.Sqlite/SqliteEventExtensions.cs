namespace Cedar.EventStore
{
    internal static class SqliteEventExtensions
    {
        internal static StreamEvent ToStreamEvent(this SqliteEvent @event)
        {
            return new StreamEvent(
                @event.OriginalStreamId,
                @event.EventId,
                @event.SequenceNumber,
                @event.Checkpoint.ToString(),
                @event.Created,
                @event.Type,
                @event.JsonData,
                @event.JsonMetadata);
        }
    }
}