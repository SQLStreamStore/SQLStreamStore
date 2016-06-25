namespace Cedar.EventStore.Infrastructure
{
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using EnsureThat;

    public abstract class EventStoreBase : ReadOnlyEventStoreBase, IEventStore
    {
        protected EventStoreBase(string logName = null)
            : base(logName)
        {}

        public Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(streamId, nameof(streamId)).DoesNotStartWith("$");

            return AppendToStreamInternal(streamId, expectedVersion, events, cancellationToken);
        }

        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(streamId, nameof(streamId)).DoesNotStartWith("$");

            return DeleteStreamInternal(streamId, expectedVersion, cancellationToken);
        }

        protected abstract Task AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken);

        protected abstract Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken);
    }
}