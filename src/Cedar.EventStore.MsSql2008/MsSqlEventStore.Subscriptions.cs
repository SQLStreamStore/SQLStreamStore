namespace Cedar.EventStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    public sealed partial class MsSqlEventStore
    {
        public async Task<IStreamSubscription> SubscribeToStream(
            string streamId,
            EventReceived eventReceived,
            SubscriptionDropped subscriptionDropped,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var watcher = await _lazySqlEventsWatcher.Value;

            return watcher.SubscribeToStream(streamId, eventReceived, subscriptionDropped);
        }
    }
}