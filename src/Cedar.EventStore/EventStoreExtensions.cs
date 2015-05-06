namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public static class EventStoreExtensions
    {
        public static Task AppendToStream(
            this IEventStore eventStore,
            string streamId,
            int expectedVersion,
            NewStreamEvent newStreamEvent)
        {
            return AppendToStream(eventStore, DefaultStore.StoreId, streamId, expectedVersion, newStreamEvent );
        }

        public static Task AppendToStream(
            this IEventStore eventStore,
            string storeId,
            string streamId,
            int expectedVersion,
            NewStreamEvent newStreamEvent)
        {
            return eventStore.AppendToStream(storeId, streamId, expectedVersion, new[] { newStreamEvent });
        }

        public static Task AppendToStream(
            this IEventStore eventStore,
            string streamId,
            int expectedVersion,
            IEnumerable<NewStreamEvent> events)
        {
            return eventStore.AppendToStream(DefaultStore.StoreId, streamId, expectedVersion, events);
        }

        public static Task DeleteStream(
            this IEventStore eventStore,
            string streamId,
            int expectedVersion = ExpectedVersion.Any)
        {
            return eventStore.DeleteStream(DefaultStore.StoreId, streamId, expectedVersion);
        }

        public static Task<AllEventsPage> ReadAll(
            this IEventStore eventStore,
            string checkpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward)
        {
            return eventStore.ReadAll(DefaultStore.StoreId, checkpoint, maxCount, direction);
        }

        public static Task<StreamEventsPage> ReadStream(
            this IEventStore eventStore,
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward)
        {
            return eventStore.ReadStream(DefaultStore.StoreId, streamId, start, count, direction);
        }
    }
}