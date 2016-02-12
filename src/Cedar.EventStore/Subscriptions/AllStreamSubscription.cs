namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;

    public sealed class AllStreamSubscription : IAllStreamSubscription
    {
        private readonly IReadOnlyEventStore _readOnlyEventStore;
        private readonly IObservable<Unit> _eventStoreAppendedNotification;
        private readonly StreamEventReceived _streamEventReceived;
        private readonly SubscriptionDropped _subscriptionDropped;
        private readonly CancellationTokenSource _isDisposed = new CancellationTokenSource();
        private long? _lastCheckpoint;
        private readonly InterlockedBoolean _isFetching = new InterlockedBoolean();
        private int _pageSize = 50;
        private IDisposable _eventStoreAppendedSubscription;

        public AllStreamSubscription(
            long? fromCheckpoint,
            IReadOnlyEventStore readOnlyEventStore,
            IObservable<Unit> eventStoreAppendedNotification,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null)
        {
            _lastCheckpoint = fromCheckpoint;
            _readOnlyEventStore = readOnlyEventStore;
            _streamEventReceived = streamEventReceived;
            _eventStoreAppendedNotification = eventStoreAppendedNotification;
            _subscriptionDropped = subscriptionDropped ?? ((_, __) => { });
            Name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;
        }

        public string Name { get; }

        public long? LastCheckpoint => _lastCheckpoint;

        public int PageSize
        {
            get { return _pageSize; }
            set { _pageSize = (value <= 0) ? 1 : value; }
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            if(_lastCheckpoint == Checkpoint.End)
            {
                // Get the last stream version and subscribe from there.
                var eventsPage = await _readOnlyEventStore.ReadAll(
                    Checkpoint.End,
                    1,
                    ReadDirection.Forward,
                    cancellationToken).NotOnCapturedContext();
                _lastCheckpoint = eventsPage.NextCheckpoint;
            }
            _eventStoreAppendedSubscription = _eventStoreAppendedNotification.Subscribe(_ => Fetch());
            Fetch();
        }

        public void Dispose()
        {
            _eventStoreAppendedSubscription?.Dispose();
            _isDisposed.Cancel();
        }

        public void Fetch()
        {
            if (_isFetching.CompareExchange(true, false))
            {
                return;
            }

            Task.Run(async () =>
            {
                bool isEnd = false;
                while (!isEnd)
                {
                    var allEventsPage = await _readOnlyEventStore
                        .ReadAll(
                            _lastCheckpoint.Value,
                            _pageSize,
                            ReadDirection.Forward,
                            _isDisposed.Token)
                        .NotOnCapturedContext();
                    isEnd = allEventsPage.IsEnd;

                    foreach (var streamEvent in allEventsPage.StreamEvents)
                    {
                        if(_isDisposed.IsCancellationRequested)
                        {
                            return;
                        }
                        _lastCheckpoint = streamEvent.Checkpoint;
                        try
                        {
                            await _streamEventReceived(streamEvent).NotOnCapturedContext();
                        }
                        catch (Exception ex)
                        {
                            try
                            {
                                _subscriptionDropped.Invoke(ex.Message, ex);
                            }
                            catch
                            {
                                //TODO logging
                            }
                            finally
                            {
                                Dispose();
                            }
                        }
                    }
                }
                _isFetching.Set(false);
            }, _isDisposed.Token);
        }
    }
}
