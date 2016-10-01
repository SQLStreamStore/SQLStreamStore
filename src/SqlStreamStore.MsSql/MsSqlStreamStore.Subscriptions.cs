namespace SqlStreamStore
{
    using SqlStreamStore.Subscriptions;

    public sealed partial class MsSqlStreamStore
    {
        protected override IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? continueAfterVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name)
        {
            return new StreamSubscription(
                streamId,
                continueAfterVersion,
                this,
                GetStoreObservable,
                streamMessageReceived,
                subscriptionDropped);
        }

        protected override IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name)
        {
            return new AllStreamSubscription(
                fromPosition,
                this,
                GetStoreObservable,
                streamMessageReceived,
                subscriptionDropped, 
                name);
        }
    }
}