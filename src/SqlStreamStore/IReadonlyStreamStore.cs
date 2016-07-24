namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public interface IReadonlyStreamStore : IDisposable
    {
        Task<AllEventsPage> ReadAllForwards(
            long fromCheckpointInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<AllEventsPage> ReadAllBackwards(
            long fromCheckpointInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<StreamMessagesPage> ReadStreamForwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<StreamMessagesPage> ReadStreamBackwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<IStreamSubscription> SubscribeToStream(
            string streamId,
            int fromVersionExclusive,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<IAllStreamSubscription> SubscribeToAll(
            long? fromCheckpointExclusive,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<long> ReadHeadCheckpoint(CancellationToken cancellationToken = default(CancellationToken));

        Task<StreamMetadataResult> GetStreamMetadata(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken));
    }
}