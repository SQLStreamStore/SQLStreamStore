namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;

    public sealed class StreamSubscription : IStreamSubscription
    {
        private readonly string _streamId;
        private readonly IReadOnlyEventStore _readOnlyEventStore;
        private readonly IObservable<Unit> _eventStoreAppendedNotification;
        private readonly StreamEventReceived _streamEventReceived;
        private readonly SubscriptionDropped _subscriptionDropped;
        private readonly CancellationTokenSource _isDisposed = new CancellationTokenSource();
        private int _currentVersion;
        private readonly InterlockedBoolean _isFetching = new InterlockedBoolean();
        private readonly InterlockedBoolean _shouldFetch = new InterlockedBoolean();
        private int _pageSize = 50;
        private IDisposable _eventStoreAppendedSubscription;
        private static readonly SubscriptionDropped s_noopSubscriptionDropped = (_, __) => { };

        public StreamSubscription(
            string streamId,
            int startVersion,
            IReadOnlyEventStore readOnlyEventStore,
            IObservable<Unit> eventStoreAppendedNotification,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped,
            string name = null)
        {
            _streamId = streamId;
            _currentVersion = startVersion;
            _readOnlyEventStore = readOnlyEventStore;
            _eventStoreAppendedNotification = eventStoreAppendedNotification;
            _streamEventReceived = streamEventReceived;
            _subscriptionDropped = subscriptionDropped ?? s_noopSubscriptionDropped;
            Name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;
        }

        public string Name { get; }

        public string StreamId => _streamId;

        public int LastVersion => _currentVersion;

        public int PageSize
        {
            get { return _pageSize; }
            set {
                if(value <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(PageSize), "Must be greater than zero");
                }
                _pageSize = value;
            }
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            if(_currentVersion == StreamVersion.End)
            {
                // Get the last stream version and subscribe from there.
                var eventsPage = await _readOnlyEventStore.ReadStream(
                    _streamId,
                    StreamVersion.End,
                    1,
                    ReadDirection.Backward,
                    cancellationToken).NotOnCapturedContext();

                //Only new events, i.e. the one after the current last one 
                _currentVersion = eventsPage.LastStreamVersion + 1;
            }
            _eventStoreAppendedSubscription = _eventStoreAppendedNotification.Subscribe(_ =>
            {
                _shouldFetch.Set(true);
                Fetch();
            });
            Fetch();
        }

        public void Dispose()
        {
            _eventStoreAppendedSubscription?.Dispose();
            _isDisposed.Cancel();
        }

        private void Fetch()
        {
            if (_isFetching.CompareExchange(true, false))
            {
                return;
            }
            Task.Run(async () =>
            {
                bool isEnd = false;
                while(!isEnd || _shouldFetch.CompareExchange(false, true))
                {
                    var streamEventsPage = await _readOnlyEventStore
                        .ReadStream(
                            _streamId,
                            _currentVersion,
                            _pageSize,
                            ReadDirection.Forward,
                            _isDisposed.Token)
                        .NotOnCapturedContext();
                    isEnd = streamEventsPage.IsEndOfStream;

                    foreach(var streamEvent in streamEventsPage.Events)
                    {
                        if(_isDisposed.IsCancellationRequested)
                        {
                            return;
                        }
                        _currentVersion = streamEvent.StreamVersion;
                        try
                        {
                            await _streamEventReceived(streamEvent).NotOnCapturedContext();
                        }
                        catch(Exception ex)
                        {
                            try
                            {
                                _subscriptionDropped.Invoke(ex.Message, ex);
                            }
                            catch(Exception ex2)
                            {
                                // Need to log this 
                            }
                            finally
                            {
                                Dispose();
                            }
                        }
                    }
                    _currentVersion++; // We want to start 
                }
                _isFetching.Set(false);
            }, _isDisposed.Token);
        }
    }
}
