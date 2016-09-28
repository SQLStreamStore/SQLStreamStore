namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Nito.AsyncEx;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.Streams;

    public sealed class StreamSubscription: IStreamSubscription
    {
        public const int DefaultPageSize = 50;
        private static readonly ILog s_logger = LogProvider.GetCurrentClassLogger();
        private int _pageSize = DefaultPageSize;
        private int _nextVersion;
        private readonly IReadonlyStreamStore _readonlyStreamStore;
        private readonly StreamMessageReceived _streamMessageReceived;
        private readonly SubscriptionDropped _subscriptionDropped;
        private readonly IDisposable _notification;
        private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
        private readonly AsyncAutoResetEvent _streamStoreNotification = new AsyncAutoResetEvent();
        private readonly TaskCompletionSource _started = new TaskCompletionSource();

        public StreamSubscription(
            string streamId,
            int startVersion,
            IReadonlyStreamStore readonlyStreamStore,
            IObservable<Unit> streamStoreAppendedNotification,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name = null)
        {
            StreamId               = streamId;
            _nextVersion           = startVersion;
            LastVersion            = startVersion - 1;
            _readonlyStreamStore   = readonlyStreamStore;
            _streamMessageReceived = streamMessageReceived;
            _subscriptionDropped   = subscriptionDropped ?? ((_, __) => { });
            Name                   = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;

            _notification = streamStoreAppendedNotification.Subscribe(_ =>
            {
                _streamStoreNotification.Set();
            });

            Task.Run(PullAndPush);

            s_logger.Info($"Stream subscription created {name}/{streamId}.");
        }

        public string Name { get; }

        public string StreamId { get; }

        public int LastVersion { get; private set; }

        public Task Started => _started.Task;

        public int PageSize
        {
            get { return _pageSize; }
            set { _pageSize = (value <= 0) ? 1 : value; }
        }

        public void Dispose()
        {
            if (_disposed.IsCancellationRequested)
            {
                return;
            }
            _disposed.Cancel();
            _notification.Dispose();
        }

        private async Task PullAndPush()
        {
            if(_nextVersion == StreamVersion.End)
            {
                await Initialize();
            }
            _started.SetResult();
            while(true)
            {
                bool pause = false;

                while(!pause)
                {
                    var streamMessagesPage = await Pull();

                    if(streamMessagesPage.Status != PageReadStatus.Success)
                    {
                        break;
                    }

                    await Push(streamMessagesPage);

                    pause = streamMessagesPage.IsEndOfStream && streamMessagesPage.Messages.Length == 0;
                }

                // Wait for notification before starting again. 
                try
                {
                    await _streamStoreNotification.WaitAsync(_disposed.Token);
                }
                catch(TaskCanceledException)
                {
                    NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                    throw;
                }
            }
        }

        private async Task Initialize()
        {
            StreamMessagesPage eventsPage;
            try
            {
                // Get the last stream version and subscribe from there.
                eventsPage = await _readonlyStreamStore.ReadStreamBackwards(
                    StreamId,
                    StreamVersion.End,
                    1,
                    _disposed.Token).NotOnCapturedContext();
            }
            catch (TaskCanceledException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch (Exception ex)
            {
                s_logger.ErrorException($"Error reading stream {Name}/{StreamId}", ex);
                NotifySubscriptionDropped(SubscriptionDroppedReason.ServerError, ex);
                throw;
            }

            //Only new Messages, i.e. the one after the current last one 
            _nextVersion = eventsPage.LastStreamVersion + 1;
        }

        private async Task<StreamMessagesPage> Pull()
        {
            StreamMessagesPage streamMessagesPage;
            try
            {
                streamMessagesPage = await _readonlyStreamStore
                    .ReadStreamForwards(
                        StreamId,
                        _nextVersion,
                        PageSize,
                        _disposed.Token)
                    .NotOnCapturedContext();
            }
            catch (TaskCanceledException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch (Exception ex)
            {
                s_logger.ErrorException($"Error reading stream {Name}/{StreamId}", ex);
                NotifySubscriptionDropped(SubscriptionDroppedReason.ServerError, ex);
                throw;
            }
            return streamMessagesPage;
        }

        private async Task Push(StreamMessagesPage streamMessagesPage)
        {
            foreach (var message in streamMessagesPage.Messages)
            {
                if (_disposed.IsCancellationRequested)
                {
                    NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                    _disposed.Token.ThrowIfCancellationRequested();
                }
                _nextVersion = message.StreamVersion + 1;
                LastVersion = message.StreamVersion;
                try
                {
                    await _streamMessageReceived(message).NotOnCapturedContext();
                }
                catch (Exception ex)
                {
                    s_logger.ErrorException(
                        $"Exception with subscriber receiving message {Name}/{StreamId}" +
                        $"Message: {message}.",
                        ex);
                    NotifySubscriptionDropped(SubscriptionDroppedReason.SubscriberError, ex);
                }
            }
        }

        private void NotifySubscriptionDropped(SubscriptionDroppedReason reason, Exception exception = null)
        {
            try
            {
                s_logger.InfoException($"Subscription dropped {Name}/{StreamId}. Reason: {reason}", exception);
                _subscriptionDropped.Invoke(reason, exception);
            }
            catch (Exception ex)
            {
                s_logger.ErrorException(
                    $"Error notifying subscriber that subscription has been dropped ({Name}/{StreamId}).",
                    ex);
            }
        }
    }
}