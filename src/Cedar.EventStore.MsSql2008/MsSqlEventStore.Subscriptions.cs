namespace Cedar.EventStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Subscriptions;

    public sealed partial class MsSqlEventStore
    {
        protected override async Task<IStreamSubscription> SubscribeToStreamInternal(
            string streamId,
            int startPosition,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        protected override Task<IAllStreamSubscription> SubscribeToAllInternal(
            long? fromCheckpoint,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }
    }
}