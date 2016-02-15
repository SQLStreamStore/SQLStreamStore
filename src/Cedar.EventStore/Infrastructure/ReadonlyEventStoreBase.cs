namespace Cedar.EventStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Cedar.EventStore.Subscriptions;

    public abstract class ReadonlyEventStoreBase : IReadOnlyEventStore
    {
        public Task<AllEventsPage> ReadAll(
            long fromCheckpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return ReadAllInternal(fromCheckpoint, maxCount, direction, cancellationToken);
        }

        protected abstract Task<AllEventsPage> ReadAllInternal(
            long fromCheckpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = new CancellationToken());

        public Task<StreamEventsPage> ReadStream(
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return ReadStreamInternal(streamId, start, count, direction, cancellationToken);
        }

        protected abstract Task<StreamEventsPage> ReadStreamInternal(
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = new CancellationToken());

        public Task<IStreamSubscription> SubscribeToStream(
            string streamId,
            int startPosition,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return SubscribeToStreamInternal(streamId,
                startPosition,
                streamEventReceived,
                subscriptionDropped,
                name,
                cancellationToken);
        }

        protected abstract Task<IStreamSubscription> SubscribeToStreamInternal(
            string streamId,
            int startPosition,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = new CancellationToken());

        public Task<IAllStreamSubscription> SubscribeToAll(
            long? fromCheckpointExclusive,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return SubscribeToAllInternal(fromCheckpointExclusive,
                streamEventReceived,
                subscriptionDropped,
                name,
                cancellationToken);
        }

        protected abstract Task<IAllStreamSubscription> SubscribeToAllInternal(
            long? fromCheckpoint,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = new CancellationToken());
    }
}