namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Timer = System.Timers.Timer;

    public sealed class Poller : IDisposable
    {
        private readonly IReadOnlyEventStore _readOnlyEventStore;
        private readonly Subject<Unit> _storeAppended = new Subject<Unit>();
        private readonly Timer _timer;
        private long _headCheckpoint = -1;
        private readonly CancellationTokenSource _disposedTokenSource = new CancellationTokenSource();

        public Poller(IReadOnlyEventStore readOnlyEventStore, int interval = 1000)
        {
            _readOnlyEventStore = readOnlyEventStore;
            _timer = new Timer(interval)
            {
                AutoReset = false
            };
            _timer.Elapsed += (_, __) => Poll().SwallowException();
        }

        public async Task Start()
        {
            _headCheckpoint = await _readOnlyEventStore.ReadHeadCheckpoint(CancellationToken.None);

            _timer.Start();
        }

        public IObservable<Unit> StoreAppended => _storeAppended;

        public void Dispose()
        {
            _disposedTokenSource.Cancel();
            _timer.Dispose();
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