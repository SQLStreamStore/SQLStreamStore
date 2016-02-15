namespace Cedar.EventStore.Infrastructure
{
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    public abstract class EventStoreBase : ReadOnlyEventStoreBase, IEventStore
    {
        public Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return AppendToStreamInternal(streamId, expectedVersion, events, cancellationToken);
        }

        protected abstract Task AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = new CancellationToken());

        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return DeleteStreamInternal(streamId, expectedVersion, cancellationToken);
        }

        protected abstract Task DeleteStreamInternal(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = new CancellationToken());
    }
}