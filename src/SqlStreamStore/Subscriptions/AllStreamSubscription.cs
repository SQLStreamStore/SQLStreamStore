﻿namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Nito.AsyncEx;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using SqlStreamStore;
    using SqlStreamStore.Logging;

    public sealed class AllStreamSubscription : IAllStreamSubscription
    {
        public const int DefaultPageSize = 10;
#if NET46
        private static readonly ILog s_logger = LogProvider.GetCurrentClassLogger();
#elif NETSTANDARD1_3
        private static readonly ILog s_logger = LogProvider.GetLogger("SqlStreamStore.Subscriptions.AllStreamSubscription");
#endif
        private int _pageSize = DefaultPageSize;
        private long _nextPosition;
        private readonly IReadonlyStreamStore _readonlyStreamStore;
        private readonly AllStreamMessageReceived _streamMessageReceived;
        private readonly bool _prefetchJsonData;
        private readonly HasCaughtUp _hasCaughtUp;
        private readonly AllSubscriptionDropped _subscriptionDropped;
        private readonly IDisposable _notification;
        private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
        private readonly AsyncAutoResetEvent _streamStoreNotification = new AsyncAutoResetEvent();
#if NET46
        private readonly TaskCompletionSource _started = new TaskCompletionSource();
#elif NETSTANDARD1_3
        private readonly TaskCompletionSource<object> _started = new TaskCompletionSource<object>();
#endif
        private readonly InterlockedBoolean _notificationRaised = new InterlockedBoolean();

        public AllStreamSubscription(
            long? continueAfterPosition,
            IReadonlyStreamStore readonlyStreamStore,
            IObservable<Unit> streamStoreAppendedNotification,
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
            Name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;

            readonlyStreamStore.OnDispose += ReadonlyStreamStoreOnOnDispose;

            _notification = streamStoreAppendedNotification.Subscribe(_ =>
            {
                _streamStoreNotification.Set();
            });

            Task.Run(PullAndPush);

            s_logger.Info($"AllStream subscription created {Name} continuing after position " +
                          $"{continueAfterPosition?.ToString() ?? "<null>"}.");
        }

        public string Name { get; }

        public long? FromPosition { get; }

        public long? LastPosition { get; private set; }

        public Task Started => _started.Task;

        public int MaxCountPerRead
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

        private void ReadonlyStreamStoreOnOnDispose()
        {
            _readonlyStreamStore.OnDispose -= ReadonlyStreamStoreOnOnDispose;
            Dispose();
        }

        private async Task PullAndPush()
        {
            if (FromPosition == Position.End)
            {
                await Initialize();
            }
#if NET46
            _started.SetResult();
#elif NETSTANDARD1_3
            _started.SetResult(null);
#endif
            while (true)
            {
                bool pause = false;
                bool? lastHasCaughtUp = null;

                while (!pause)
                {
                    var page = await Pull();

                    if(!lastHasCaughtUp.HasValue || lastHasCaughtUp.Value != page.IsEnd)
                    {
                        // Only raise if the state changes
                        lastHasCaughtUp = page.IsEnd;
                        _hasCaughtUp(page.IsEnd);
                    }

                    await Push(page);

                    pause = page.IsEnd && page.Messages.Length == 0;
                }

                // Wait for notification before starting again. 
                try
                {
                    await _streamStoreNotification.WaitAsync(_disposed.Token).NotOnCapturedContext();
                }
                catch (TaskCanceledException)
                {
                    NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                    throw;
                }
            }
        }

        private async Task Initialize()
        {
            ReadAllPage eventsPage;
            try
            {
                // Get the last stream version and subscribe from there.
                eventsPage = await _readonlyStreamStore.ReadAllBackwards(
                    Position.End,
                    1,
                    _disposed.Token).NotOnCapturedContext();
            }
            catch (ObjectDisposedException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch (OperationCanceledException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch (Exception ex)
            {
                s_logger.ErrorException($"Error reading stream {Name}", ex);
                NotifySubscriptionDropped(SubscriptionDroppedReason.StreamStoreError, ex);
                throw;
            }

            // Only new Messages, i.e. the one after the current last one.
            // Edge case for empty store where Next position 0 (when FromPosition = 0)
            _nextPosition = eventsPage.FromPosition == 0 ? 0 : eventsPage.FromPosition + 1;
        }

        private async Task<ReadAllPage> Pull()
        {
            ReadAllPage readAllPage;
            try
            {
                readAllPage = await _readonlyStreamStore
                    .ReadAllForwards(_nextPosition, MaxCountPerRead, _prefetchJsonData, _disposed.Token)
                    .NotOnCapturedContext();
            }
            catch(ObjectDisposedException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch (OperationCanceledException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch (Exception ex)
            {
                s_logger.ErrorException($"Error reading all stream {Name}", ex);
                NotifySubscriptionDropped(SubscriptionDroppedReason.StreamStoreError, ex);
                throw;
            }
            return readAllPage;
        }

        private async Task Push(ReadAllPage page)
        {
            foreach (var message in page.Messages)
            {
                if (_disposed.IsCancellationRequested)
                {
                    NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                    _disposed.Token.ThrowIfCancellationRequested();
                }
                _nextPosition = message.Position + 1;
                LastPosition = message.Position;
                try
                {
                    await _streamMessageReceived(this, message).NotOnCapturedContext();
                }
                catch (Exception ex)
                {
                    s_logger.ErrorException(
                        $"Exception with subscriber receiving message {Name}" +
                        $"Message: {message}.",
                        ex);
                    NotifySubscriptionDropped(SubscriptionDroppedReason.SubscriberError, ex);
                    throw;
                }
            }
        }

        private void NotifySubscriptionDropped(SubscriptionDroppedReason reason, Exception exception = null)
        {
            if (_notificationRaised.CompareExchange(true, false))
            {
                return;
            }
            try
            {
                s_logger.InfoException($"All stream subscription dropped {Name}. Reason: {reason}", exception);
                _subscriptionDropped.Invoke(this, reason, exception);
            }
            catch (Exception ex)
            {
                s_logger.ErrorException(
                    $"Error notifying subscriber that subscription has been dropped ({Name}).",
                    ex);
            }
        }
    }
}
