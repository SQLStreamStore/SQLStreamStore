namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using SqlStreamStore;
    using SqlStreamStore.Logging;

    /// <summary>
    ///     Represents a subscription to all streams.
    /// </summary>
    public sealed class AllStreamSubscription : IAllStreamSubscription
    {
        public const int DefaultPageSize = 10;

        private static readonly ILog s_logger =
            LogProvider.GetLogger("SqlStreamStore.Subscriptions.AllStreamSubscription");

        private int _pageSize = DefaultPageSize;
        private long _nextPosition;
        private readonly IReadonlyStreamStore _readonlyStreamStore;
        private readonly AllStreamMessageReceived _streamMessageReceived;
        private readonly bool _prefetchJsonData;
        private readonly HasCaughtUp _hasCaughtUp;
        private readonly AllSubscriptionDropped _subscriptionDropped;
        private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
        private readonly TaskCompletionSource<object> _started = new TaskCompletionSource<object>();
        private readonly InterlockedBoolean _notificationRaised = new InterlockedBoolean();
        private readonly CancellationTokenRegistration _disposedRegistration;

        public AllStreamSubscription(
            long? continueAfterPosition,
            IReadonlyStreamStore readonlyStreamStore,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name)
        {
            FromPosition = continueAfterPosition;
            LastPosition = continueAfterPosition;
            _nextPosition = continueAfterPosition + 1 ?? Position.Start;
            _readonlyStreamStore = readonlyStreamStore;
            _streamMessageReceived = streamMessageReceived;
            _prefetchJsonData = prefetchJsonData;
            _subscriptionDropped = subscriptionDropped ?? ((_, __, ___) => { });
            _hasCaughtUp = hasCaughtUp ?? (_ => { });
            _disposedRegistration = _disposed.Token.Register(
                () => NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed));
            Name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;

            readonlyStreamStore.OnDispose += ReadonlyStreamStoreOnOnDispose;

            Task.Run(PullAndPush);

            s_logger.Info(
                "AllStream subscription created {name} continuing after position {position}",
                Name,
                continueAfterPosition?.ToString() ?? "<null>");
        }

        /// <inheritdoc />
        public string Name { get; }

        /// <inheritdoc />
        public long? FromPosition { get; }

        /// <inheritdoc />
        public long? LastPosition { get; private set; }

        /// <inheritdoc />
        public Task Started => _started.Task;

        /// <inheritdoc />
        public int MaxCountPerRead
        {
            get => _pageSize;
            set => _pageSize = value <= 0 ? 1 : value;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if(_disposed.IsCancellationRequested)
            {
                return;
            }

            _disposed.Cancel();
            _disposedRegistration.Dispose();
        }

        private void ReadonlyStreamStoreOnOnDispose()
        {
            _readonlyStreamStore.OnDispose -= ReadonlyStreamStoreOnOnDispose;
            Dispose();
        }

        private async Task PullAndPush()
        {
            if(FromPosition == Position.End)
            {
                await Initialize();
            }

            _started.SetResult(null);

            bool lastHasCaughtUp = false;

            while(!_disposed.IsCancellationRequested)
            {
                try
                {
                    var messages = _readonlyStreamStore.ReadAllForwards(
                        _nextPosition,
                        MaxCountPerRead,
                        _prefetchJsonData,
                        _disposed.Token);

                    await foreach(var message in messages)
                    {
                        await Push(message);
                    }

                    if(messages.IsEnd && !lastHasCaughtUp)
                    {
                        _hasCaughtUp(true);
                        lastHasCaughtUp = true;
                    }
                    else if(!messages.IsEnd && lastHasCaughtUp)
                    {
                        _hasCaughtUp(false);
                        lastHasCaughtUp = false;
                    }
                }
                catch(ObjectDisposedException)
                {
                    NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                    throw;
                }
                catch(OperationCanceledException)
                {
                    NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                    throw;
                }
                catch(Exception ex)
                {
                    s_logger.ErrorException($"Error reading all stream {Name}", ex);
                    NotifySubscriptionDropped(SubscriptionDroppedReason.StreamStoreError, ex);
                    throw;
                }

                await Task.Delay(100).NotOnCapturedContext();
            }
        }

        private async Task Initialize()
        {
            try
            {
                // Get the last position and subscribe from there.
                var fromPosition = await _readonlyStreamStore.ReadHeadPosition(_disposed.Token).NotOnCapturedContext();
                _nextPosition = fromPosition == 0 ? 0 : fromPosition + 1;
            }
            catch(ObjectDisposedException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch(OperationCanceledException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch(Exception ex)
            {
                s_logger.ErrorException($"Error reading stream {Name}", ex);
                NotifySubscriptionDropped(SubscriptionDroppedReason.StreamStoreError, ex);
                throw;
            }

            // Only new Messages, i.e. the one after the current last one.
            // Edge case for empty store where Next position 0 (when FromPosition = 0)
        }

        private async Task Push(StreamMessage message)
        {
            if(_disposed.IsCancellationRequested)
            {
                return;
            }

            _nextPosition = message.Position + 1;
            LastPosition = message.Position;
            try
            {
                await _streamMessageReceived(this, message, _disposed.Token).NotOnCapturedContext();
            }
            catch(Exception ex)
            {
                s_logger.ErrorException(
                    $"Exception with subscriber receiving message {Name}" +
                    $"Message: {message}.",
                    ex);
                NotifySubscriptionDropped(SubscriptionDroppedReason.SubscriberError, ex);
                throw;
            }
        }

        private void NotifySubscriptionDropped(SubscriptionDroppedReason reason, Exception exception = null)
        {
            if(_notificationRaised.CompareExchange(true, false))
            {
                return;
            }

            try
            {
                s_logger.InfoException($"All stream subscription dropped {Name}. Reason: {reason}", exception);
                _subscriptionDropped.Invoke(this, reason, exception);
            }
            catch(Exception ex)
            {
                s_logger.ErrorException(
                    $"Error notifying subscriber that subscription has been dropped ({Name}).",
                    ex);
            }
        }
    }
}