namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Collections.Concurrent;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;
    using EnsureThat;
    using global::EventStore.ClientAPI;
    using global::EventStore.ClientAPI.SystemData;

    internal class GesAllStreamSubscription : IAllStreamSubscription
    {
        private readonly string _name;
        private long? _lastCheckpoint;
        private readonly IEventStoreConnection _connection;
        private long? _fromCheckpoint;
        private readonly StreamEventReceived _streamEventReceived;
        private readonly SubscriptionDropped _subscriptionDropped;
        private readonly UserCredentials _userCredentials;
        private long _nextCheckpoint;
        private EventStoreAllCatchUpSubscription _subscription;
        private readonly ConcurrentQueue<StreamEvent> _queue = new ConcurrentQueue<StreamEvent>();
        private const int MinQueueSize = 500;
        private const int MaxQueueSize = 5000;
        private readonly CancellationTokenSource _isDisposed = new CancellationTokenSource();

        public GesAllStreamSubscription(
            IEventStoreConnection connection,
            long? fromCheckpoint, 
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null, 
            UserCredentials userCredentials = null)
        {
            _name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;

            _connection = connection;
            _fromCheckpoint = fromCheckpoint;
            _streamEventReceived = streamEventReceived;
            _subscriptionDropped = subscriptionDropped ?? ((_, ex) => {});
            _userCredentials = userCredentials;
            _lastCheckpoint = fromCheckpoint;
            _nextCheckpoint = fromCheckpoint + 1 ?? Checkpoint.Start;
        }

        public string Name => _name;
        public long? LastCheckpoint => _lastCheckpoint;

        private Position? LastCheckpointPosition
            => _lastCheckpoint.HasValue
                    ? new Position(_lastCheckpoint.Value, _lastCheckpoint.Value)
                    : default(Position?);

        public Task Start(CancellationToken cancellationToken)
        {
            _subscription = _connection.SubscribeToAllFrom(LastCheckpointPosition, 
                false,
                EventAppeared,
                subscriptionDropped: SubscriptionDropped,
                userCredentials: _userCredentials);

            return ProcessQueue();
        }

        private void SubscriptionDropped(EventStoreCatchUpSubscription _, SubscriptionDropReason reason, Exception ex)
        {
            if(reason == SubscriptionDropReason.UserInitiated)
            {
                return;
            }

            _subscriptionDropped(reason.ToString(), ex);
        }

        private void EventAppeared(EventStoreCatchUpSubscription _, ResolvedEvent resolvedEvent)
        {
            if(resolvedEvent.Event.EventType.StartsWith("$"))
            {
                return;
            }

            if(_queue.Count >= MaxQueueSize)
            {
                _subscription.Stop();

                return;
            }

            var streamEvent = new StreamEvent(
                resolvedEvent.Event.EventStreamId, 
                resolvedEvent.Event.EventId, 
                resolvedEvent.Event.EventNumber, 
                _subscription.LastProcessedPosition.PreparePosition,
                resolvedEvent.Event.Created,
                resolvedEvent.Event.EventType,
                Encoding.UTF8.GetString(resolvedEvent.Event.Data),
                Encoding.UTF8.GetString(resolvedEvent.Event.Metadata));

            _queue.Enqueue(streamEvent);
        }

        private Task ProcessQueue()
        {
            if(_isDisposed.IsCancellationRequested)
            {
                return Task.FromResult(0);
            }
            StreamEvent streamEvent;
            if(_queue.TryDequeue(out streamEvent))
            {
                return _streamEventReceived(streamEvent)
                    .ContinueWith(_ => ProcessQueue());
            }

            return Task.Delay(1).ContinueWith(_ => ProcessQueue());
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if(!disposing)
            {
                return;
            }

            _subscription?.Stop();
            _isDisposed.Cancel();
        }

        ~GesAllStreamSubscription()
        {
            Dispose(false);
        }
    }
}
