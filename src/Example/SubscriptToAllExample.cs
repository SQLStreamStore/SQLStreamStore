
namespace Example
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using SqlStreamStore;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public class SubscriptToAllExample : IDisposable
    {
        private readonly IStreamStore _streamStore;
        private IAllStreamSubscription _subscription;
        private long? _currentPosition;

        public SubscriptToAllExample(IStreamStore streamStore)
        {
            _streamStore = streamStore;
            _subscription = streamStore.SubscribeToAll(null, StreamMessageReceived, SubscriptionDropped, IsCaughtUp);
        }

        public bool CaughtUp { get; set; }

        private void IsCaughtUp(bool isCaughtUp)
        {
            CaughtUp = isCaughtUp;
        }

        private void SubscriptionDropped(IAllStreamSubscription _, SubscriptionDroppedReason reason, Exception exception)
        {
            if(reason == SubscriptionDroppedReason.StreamStoreError)
            {
                // Transient errors should re-create the subscription
                if(exception is SqlException)
                {
                    _subscription = _streamStore.SubscribeToAll(_currentPosition, StreamMessageReceived, SubscriptionDropped, IsCaughtUp);
                }
            }
        }

        private Task StreamMessageReceived(IAllStreamSubscription _, StreamMessage streamMessage)
        {
            Console.WriteLine(streamMessage.StreamId);
            _currentPosition = streamMessage.Position;
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _subscription.Dispose();
        }
    }
}
