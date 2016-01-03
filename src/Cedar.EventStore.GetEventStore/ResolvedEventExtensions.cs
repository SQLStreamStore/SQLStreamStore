namespace Cedar.EventStore
{
    using System.Text;
    using Cedar.EventStore.Streams;
    using global::EventStore.ClientAPI;

    internal static class ResolvedEventExtensions
    {
        internal static StreamEvent ToSteamEvent(this ResolvedEvent resolvedEvent)
        {
            return new StreamEvent(
                resolvedEvent.Event.EventStreamId,
                resolvedEvent.Event.EventId,
                resolvedEvent.Event.EventNumber,
                resolvedEvent.OriginalPosition.ToString(),
                resolvedEvent.Event.Created,
                resolvedEvent.Event.EventType,
                Encoding.UTF8.GetString(resolvedEvent.Event.Data),
                Encoding.UTF8.GetString(resolvedEvent.Event.Metadata));
        }
    }
}