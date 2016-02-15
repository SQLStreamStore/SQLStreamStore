namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;


    public sealed class AllStreamSubscription : IAllStreamSubscription
    {
        private readonly IReadOnlyEventStore _readOnlyEventStore;
        private readonly IObservable<Unit> _eventStoreAppendedNotification;
        private readonly StreamEventReceived _streamEventReceived;
        private readonly SubscriptionDropped _subscriptionDropped;
        private readonly CancellationTokenSource _isDisposed = new CancellationTokenSource();
        private long _nextCheckpoint;
        private readonly InterlockedBoolean _isFetching = new InterlockedBoolean();
        private readonly InterlockedBoolean _shouldFetch = new InterlockedBoolean();
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
            FromCheckpoint = fromCheckpoint;
            LastCheckpoint = fromCheckpoint;
            _nextCheckpoint = fromCheckpoint + 1 ?? Checkpoint.Start;
            _readOnlyEventStore = readOnlyEventStore;
            _streamEventReceived = streamEventReceived;
            _eventStoreAppendedNotification = eventStoreAppendedNotification;
            _subscriptionDropped = subscriptionDropped ?? ((_, __) => { });
            Name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;
        }

        public string Name { get; }

        public long? FromCheckpoint { get; }

        public long? LastCheckpoint { get; private set; }

        public int PageSize
        {
            get { return _pageSize; }
            set { _pageSize = (value <= 0) ? 1 : value; }
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            if(FromCheckpoint == Checkpoint.End)
            {
                // Get the last stream version and subscribe from there.
                var eventsPage = await _readOnlyEventStore.ReadAllBackwards(
                    Checkpoint.End,
                    1,
                    cancellationToken).NotOnCapturedContext();

                // If fromCheckpoint = 0, we have empty store, so start from zero, otherwise, the next checkpoint is 
                // one after the FromCheckpoint.
                _nextCheckpoint = eventsPage.FromCheckpoint == 0 ?  0 : eventsPage.FromCheckpoint + 1;
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
                while (!isEnd || _shouldFetch.CompareExchange(false, true))
                {
                    Console.WriteLine($"Fetching from {_nextCheckpoint}");
                    var allEventsPage = await _readOnlyEventStore
                        .ReadAllForwards(
                            _nextCheckpoint,
                            _pageSize,
                            _isDisposed.Token)
                        .NotOnCapturedContext();
                    isEnd = allEventsPage.IsEnd;

                    Console.WriteLine($"Received {allEventsPage.StreamEvents.Length} events");
                    foreach (var streamEvent in allEventsPage.StreamEvents)
                    {
                        if(_isDisposed.IsCancellationRequested)
                        {
                            return;
                        }
                        try
                        {
                            await _streamEventReceived(streamEvent).NotOnCapturedContext();
                            LastCheckpoint = streamEvent.Checkpoint;
                            _nextCheckpoint = streamEvent.Checkpoint + 1;
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
                    Console.WriteLine($"LastCheckpoint {LastCheckpoint}");
                }
                _isFetching.Set(false);
            }, _isDisposed.Token);
        }
    }
}
