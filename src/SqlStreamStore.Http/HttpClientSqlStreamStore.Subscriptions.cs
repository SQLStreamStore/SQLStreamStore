namespace SqlStreamStore
{
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    partial class HttpClientSqlStreamStore
    {
        public IStreamSubscription SubscribeToStream(
            StreamId streamId,
            int? continueAfterVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            bool prefetchJsonData = true,
            string name = null)
        {
            GuardAgainstDisposed();

            return new StreamSubscription(
                streamId,
                continueAfterVersion,
                this,
                _streamStoreNotifier.Value,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);
        }

        public IAllStreamSubscription SubscribeToAll(
            long? continueAfterPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            bool prefetchJsonData = true,
            string name = null)
        {
            GuardAgainstDisposed();

            return new AllStreamSubscription(
                continueAfterPosition,
                this,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);
        }
    }
}