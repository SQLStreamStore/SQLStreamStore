namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public static class EventStoreExtensions
    {
        public static Task AppendToStream(
            this IEventStoreClient eventStoreClient,
            string streamId,
            int expectedVersion,
            NewStreamEvent newStreamEvent)
        {
            return eventStoreClient.AppendToStream(streamId, expectedVersion, new[] { newStreamEvent });
        }

        public static Task AppendToStream(
            this IEventStoreClient eventStoreClient,
            string streamId,
            int expectedVersion,
            IEnumerable<NewStreamEvent> events)
        {
            return eventStoreClient.AppendToStream(streamId, expectedVersion, events);
        }
    }
}