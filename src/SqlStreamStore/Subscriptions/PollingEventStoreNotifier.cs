namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore;
    using Timer = System.Timers.Timer;

    public sealed class PollingEventStoreNotifier : IEventStoreNotifier
    {
        public static CreateEventStoreNotifier CreateEventStoreNotifier(int interval = 1000)
        {
            return async readOnlyEventStore =>
            {
                var poller = new PollingEventStoreNotifier(readOnlyEventStore, interval);
                await poller.Start().NotOnCapturedContext();
                return poller;
            };
        }

        private readonly CancellationTokenSource _disposedTokenSource = new CancellationTokenSource();
        private readonly IReadOnlyEventStore _readOnlyEventStore;
        private readonly Subject<Unit> _storeAppended = new Subject<Unit>();
        private readonly Timer _timer;
        private long _headCheckpoint = -1;

        public PollingEventStoreNotifier(IReadOnlyEventStore readOnlyEventStore, int interval = 1000)
        {
            _readOnlyEventStore = readOnlyEventStore;
            _timer = new Timer(interval)
            {
                AutoReset = false
            };
            _timer.Elapsed += (_, __) => Poll().SwallowException();
        }

        public void Dispose()
        {
            _disposedTokenSource.Cancel();
            _timer.Dispose();
        }

        public IDisposable Subscribe(IObserver<Unit> observer)
        {
            return _storeAppended.Subscribe(observer);
        }

        public async Task Start()
        {
            _headCheckpoint = await _readOnlyEventStore.ReadHeadCheckpoint(CancellationToken.None);

            _timer.Start();
        }

        private async Task Poll()
        {
            // TODO try-catch-log
            var headCheckpoint = await _readOnlyEventStore.ReadHeadCheckpoint(_disposedTokenSource.Token);

            if(headCheckpoint > _headCheckpoint)
            {
                _storeAppended.OnNext(Unit.Default);
                _headCheckpoint = headCheckpoint;
            }

            _timer.Start();
        }
    }
}