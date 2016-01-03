namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    public static class EventStoreExtensions
    {
        public static Task AppendToStream(
            this IEventStore eventStore,
            string streamId,
            int expectedVersion,
            NewStreamEvent newStreamEvent)
        {
            return eventStore.AppendToStream(streamId, expectedVersion, new[] { newStreamEvent });
        }

        public static Task AppendToStream(
            this IEventStore eventStore,
            string streamId,
            int expectedVersion,
            IEnumerable<NewStreamEvent> events)
        {
            return eventStore.AppendToStream(streamId, expectedVersion, events);
        }
    }
}