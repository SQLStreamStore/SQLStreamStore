namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;

    public sealed class PollingStreamStoreNotifier : IStreamStoreNotifier
    {
#if NET46
        private static readonly ILog s_logger = LogProvider.GetCurrentClassLogger();
#elif NETSTANDARD1_3
        private static readonly ILog s_logger = LogProvider.GetLogger("SqlStreamStore.Subscriptions.PollingStreamStoreNotifier");
#endif
        private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
        private readonly Func<CancellationToken, Task<long>> _readHeadPosition;
        private readonly int _interval;
        private readonly Subject<Unit> _storeAppended = new Subject<Unit>();

        public PollingStreamStoreNotifier(IReadonlyStreamStore readonlyStreamStore, int interval = 1000)
            : this(readonlyStreamStore.ReadHeadPosition, interval)
        {}

        public PollingStreamStoreNotifier(Func<CancellationToken, Task<long>> readHeadPosition, int interval = 1000)
        {
            _readHeadPosition = readHeadPosition;
            _interval = interval;
            Task.Run(Poll, _disposed.Token);
        }

        public void Dispose()
        {
            _disposed.Cancel();
        }

        public IDisposable Subscribe(IObserver<Unit> observer) => _storeAppended.Subscribe(observer);

        private async Task Poll()
        {
            long headPosition = -1;
            long previousHeadPosition = headPosition;
            while (!_disposed.IsCancellationRequested)
            {
                try
                {
                    headPosition = await _readHeadPosition(_disposed.Token);
                    if(s_logger.IsTraceEnabled())
                    {
                        s_logger.TraceFormat("Polling head position {headPosition}. Previous {previousHeadPosition}",
                            headPosition, previousHeadPosition);
                    }
                }
                catch(Exception ex)
                {
                    s_logger.ErrorException($"Exception occurred polling stream store for messages. " +
                                            $"HeadPosition: {headPosition}", ex);
                }

                if(headPosition > previousHeadPosition)
                {
                    _storeAppended.OnNext(Unit.Default);
                    previousHeadPosition = headPosition;
                }
                else
                {
                    await Task.Delay(_interval, _disposed.Token);
                }
            }
        }
    }
}