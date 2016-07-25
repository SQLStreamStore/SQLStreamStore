namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public interface IReadonlyStreamStore : IDisposable
    {
        Task<AllMessagesPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<AllMessagesPage> ReadAllBackwards(
            long fromPositionInclusive,
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
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<IAllStreamSubscription> SubscribeToAll(
            long? fromPositionExclusive,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<long> ReadHeadPosition(CancellationToken cancellationToken = default(CancellationToken));

        Task<StreamMetadataResult> GetStreamMetadata(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken));
    }
}