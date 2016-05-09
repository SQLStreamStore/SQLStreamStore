namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Collections.Concurrent;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using global::EventStore.ClientAPI;
    using global::EventStore.ClientAPI.SystemData;

    internal class GesStreamSubscription : IStreamSubscription
    {
        private readonly string _name;
        private readonly IEventStoreConnection _connection;
        private readonly string _streamId;
        private readonly int? _startVersionExclusive;
        private readonly StreamEventReceived _streamEventReceived;
        private readonly SubscriptionDropped _subscriptionDropped;
        private readonly UserCredentials _userCredentials;
        private readonly ConcurrentQueue<StreamEvent> _queue = new ConcurrentQueue<StreamEvent>();
        private const int MinQueueSize = 500;
        private const int MaxQueueSize = 5000;
        private readonly CancellationTokenSource _isDisposed = new CancellationTokenSource();
        private int _nextVersion;
        private int _lastVersion;
        private Action _stopSubscription;
        private readonly int _startVersion;


        public string Name => _name;
        public string StreamId => _streamId;
        public int LastVersion => _lastVersion;

        public GesStreamSubscription(
            IEventStoreConnection connection,
            string streamId,
            int startVersion,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            UserCredentials userCredentials = null)
        {
            _connection = connection;
            _streamId = streamId;
            _startVersion = startVersion;
            _startVersionExclusive = startVersion == 0 ? default(int?) : startVersion - 1;
            _streamEventReceived = streamEventReceived;
            _subscriptionDropped = subscriptionDropped ?? ((_, ex) => {});
            _userCredentials = userCredentials;
            _name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;
            _nextVersion = startVersion;
            _lastVersion = startVersion;
        }

        public Task Start(CancellationToken cancellationToken)
        {
            var subscriptionTask = _startVersion == StreamVersion.End
                ? SubscribeToStreamFromEnd()
                : SubscribeToStreamFromStartVersion();

            return subscriptionTask.ContinueWith(_ => _stopSubscription = _.Result, cancellationToken)
                .ContinueWith(_ => ProcessQueue(), cancellationToken);
        }

        private Task<Action> SubscribeToStreamFromStartVersion()
        {
            var subscription = _connection.SubscribeToStreamFrom(
                _streamId,
                _startVersionExclusive,
                false,
                (_, e) => EventAppeared(e),
                subscriptionDropped: (_, reason, ex) => SubscriptionDropped(reason, ex),
                userCredentials: _userCredentials);

            return Task.FromResult<Action>(() => subscription.Stop());
        }

        private Task<Action> SubscribeToStreamFromEnd()
        {
            return _connection.SubscribeToStreamAsync(
                _streamId,
                true,
                (_, e) => EventAppeared(e),
                (_, reason, ex) => SubscriptionDropped(reason, ex),
                _userCredentials)
                .ContinueWith(_ => new Action(_.Result.Close));
        }

        private void SubscriptionDropped(SubscriptionDropReason reason, Exception ex)
        {
            if (reason == SubscriptionDropReason.UserInitiated)
            {
                return;
            }

            _subscriptionDropped(reason.ToString(), ex);
        }

        private void EventAppeared(ResolvedEvent resolvedEvent)
        {
            if (resolvedEvent.Event.EventType.StartsWith("$"))
            {
                return;
            }

            if (_queue.Count >= MaxQueueSize)
            {
                _stopSubscription?.Invoke();
                return;
            }

            var streamEvent = new StreamEvent(
                resolvedEvent.Event.EventStreamId,
                resolvedEvent.Event.EventId,
                resolvedEvent.Event.EventNumber,
                resolvedEvent.OriginalPosition.GetValueOrDefault().PreparePosition,
                resolvedEvent.Event.Created,
                resolvedEvent.Event.EventType,
                Encoding.UTF8.GetString(resolvedEvent.Event.Data),
                Encoding.UTF8.GetString(resolvedEvent.Event.Metadata));

            _queue.Enqueue(streamEvent);
        }

        private Task ProcessQueue()
        {
            if (_isDisposed.IsCancellationRequested)
            {
                return Task.FromResult(0);
            }
            StreamEvent streamEvent;
            if (_queue.TryDequeue(out streamEvent))
            {
                return _streamEventReceived(streamEvent)
                    .ContinueWith(_ => Interlocked.Exchange(ref _lastVersion, streamEvent.StreamVersion))
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
            if (!disposing)
            {
                return;
            }

            _stopSubscription?.Invoke();
            _isDisposed.Cancel();
        }

        ~GesStreamSubscription()
        {
            Dispose(false);
        }
    }
}