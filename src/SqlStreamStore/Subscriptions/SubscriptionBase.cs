namespace StreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using StreamStore;
    using StreamStore.Infrastructure;

    public abstract class SubscriptionBase : IDisposable
    {
        private int _pageSize = 50;
        private IDisposable _eventStoreAppendedSubscription;
        private readonly InterlockedBoolean _shouldFetch = new InterlockedBoolean();
        private readonly InterlockedBoolean _isFetching = new InterlockedBoolean();
        private readonly CancellationTokenSource _isDisposed = new CancellationTokenSource();

        protected SubscriptionBase(
            IReadOnlyEventStore readOnlyEventStore,
            IObservable<Unit> eventStoreAppendedNotification,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null)
        {
            ReadOnlyEventStore = readOnlyEventStore;
            EventStoreAppendedNotification = eventStoreAppendedNotification;
            StreamEventReceived = streamEventReceived;
            Name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;
            SubscriptionDropped = subscriptionDropped ?? ((_, __) => { });
        }

        public string Name { get; }

        public int PageSize
        {
            get { return _pageSize; }
            set { _pageSize = (value <= 0) ? 1 : value; }
        }

        protected IObservable<Unit> EventStoreAppendedNotification { get; }

        protected CancellationToken IsDisposed => _isDisposed.Token;

        protected IReadOnlyEventStore ReadOnlyEventStore { get; }

        protected StreamEventReceived StreamEventReceived { get; }

        protected SubscriptionDropped SubscriptionDropped { get; }

        public virtual Task Start(CancellationToken cancellationToken)
        {
            _eventStoreAppendedSubscription = EventStoreAppendedNotification.Subscribe(_ =>
            {
                _shouldFetch.Set(true);
                Fetch();
            });
            Fetch();
            return Task.FromResult(0);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~SubscriptionBase()
        {
            Dispose(false);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _eventStoreAppendedSubscription?.Dispose();
                _isDisposed.Cancel();
            }
        }
        private void Fetch()
        {
            if (_isFetching.CompareExchange(true, false))
            {
                return;
            }
            Task.Run(async () =>
            {
                try
                {
                    bool isEnd = false;
                    while(_shouldFetch.CompareExchange(false, true) || !isEnd)
                    {
                        isEnd = await DoFetch();
                    }
                }
                catch(Exception ex)
                {
                    // Drop subscription
                }
                finally
                {
                    _isFetching.Set(false);
                }
            }, IsDisposed);
        }

        protected abstract Task<bool> DoFetch();
    }
}
