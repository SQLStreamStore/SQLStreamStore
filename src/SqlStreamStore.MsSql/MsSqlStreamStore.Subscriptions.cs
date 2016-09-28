namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Subscriptions;

    public sealed partial class MsSqlStreamStore
    {
        protected override IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name)
        {
            return new StreamSubscription(
                streamId,
                startVersion,
                this,
                GetStoreObservable,
                streamMessageReceived,
                subscriptionDropped);
        }

        protected override async Task<IAllStreamSubscription> SubscribeToAllInternal(
            long? fromPosition,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name,
            CancellationToken cancellationToken)
        {
            var subscription = new AllStreamSubscription(
                fromPosition,
                this,
                GetStoreObservable,
                streamMessageReceived,
                subscriptionDropped, 
                name);

            await subscription.Start(cancellationToken);

            return subscription;
        }
    }
}