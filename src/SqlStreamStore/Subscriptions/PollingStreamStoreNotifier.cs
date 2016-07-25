namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore;
    using Timer = System.Timers.Timer;

    public sealed class PollingStreamStoreNotifier : IStreamStoreNotifier
    {
        public static CreateStreamStoreNotifier CreateStreamStoreNotifier(int interval = 1000)
        {
            return async readonlyStreamStore =>
            {
                var poller = new PollingStreamStoreNotifier(readonlyStreamStore, interval);
                await poller.Start().NotOnCapturedContext();
                return poller;
            };
        }

        private readonly CancellationTokenSource _disposedTokenSource = new CancellationTokenSource();
        private readonly IReadonlyStreamStore _readonlyStreamStore;
        private readonly Subject<Unit> _storeAppended = new Subject<Unit>();
        private readonly Timer _timer;
        private long _headPosition = -1;

        public PollingStreamStoreNotifier(IReadonlyStreamStore readonlyStreamStore, int interval = 1000)
        {
            _readonlyStreamStore = readonlyStreamStore;
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
            _headPosition = await _readonlyStreamStore.ReadHeadPosition(CancellationToken.None);

            _timer.Start();
        }

        private async Task Poll()
        {
            // TODO try-catch-log
            var headPosition = await _readonlyStreamStore.ReadHeadPosition(_disposedTokenSource.Token);

            if(headPosition > _headPosition)
            {
                _storeAppended.OnNext(Unit.Default);
                _headPosition = headPosition;
            }

            _timer.Start();
        }
    }
}