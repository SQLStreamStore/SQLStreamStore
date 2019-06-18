namespace SqlStreamStore.V1
{
    using System;
    using SqlStreamStore.V1.Infrastructure;
    using SqlStreamStore.V1.Subscriptions;

    partial class MySqlStreamStore
    {
        protected override IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name) => new StreamSubscription(
            streamId,
            startVersion,
            this,
            GetStoreObservable,
            streamMessageReceived,
            subscriptionDropped,
            hasCaughtUp,
            prefetchJsonData,
            name);

        protected override IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name)
            => new AllStreamSubscription(
                fromPosition,
                this,
                GetStoreObservable,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);

        private IObservable<Unit> GetStoreObservable => _streamStoreNotifier.Value;
    }
}