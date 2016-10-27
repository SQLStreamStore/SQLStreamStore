namespace SqlStreamStore
{
    using SqlStreamStore.Subscriptions;

    public sealed partial class MsSqlStreamStore
    {
        protected override IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            string name)
        {
            return new StreamSubscription(
                streamId,
                startVersion,
                this,
                GetStoreObservable,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                name);
        }

        protected override IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            string name)
        {
            return new AllStreamSubscription(
                fromPosition,
                this,
                GetStoreObservable,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                name);
        }
    }
}