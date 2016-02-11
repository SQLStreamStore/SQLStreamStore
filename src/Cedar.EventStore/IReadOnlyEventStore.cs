namespace Cedar.EventStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Cedar.EventStore.Subscriptions;

    public interface IReadOnlyEventStore
    {
        Task<AllEventsPage> ReadAll(
            string fromCheckpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<StreamEventsPage> ReadStream(
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<IStreamSubscription> SubscribeToStream(
            string streamId,
            int startPosition,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<IAllStreamSubscription> SubscribeToAll(
            string fromCheckpoint,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken));

        string StartCheckpoint { get; }

        string EndCheckpoint { get; }
    }
}