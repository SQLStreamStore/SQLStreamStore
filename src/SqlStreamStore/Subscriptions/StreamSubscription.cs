﻿namespace SqlStreamStore.Subscriptions
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
        public const int DefaultPageSize = 10;
        private static readonly ILog s_logger = LogProvider.GetCurrentClassLogger();
        private int _pageSize = DefaultPageSize;
        private int _nextVersion;
        private readonly int? _continueAfterVersion;
        private readonly IReadonlyStreamStore _readonlyStreamStore;
        private readonly StreamMessageReceived _streamMessageReceived;
        private readonly HasCaughtUp _hasCaughtUp;
        private readonly SubscriptionDropped _subscriptionDropped;
        private readonly IDisposable _notification;
        private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
        private readonly AsyncAutoResetEvent _streamStoreNotification = new AsyncAutoResetEvent();
        private readonly TaskCompletionSource _started = new TaskCompletionSource();
        private readonly InterlockedBoolean _notificationRaised = new InterlockedBoolean();

        public StreamSubscription(
            string streamId,
            int? continueAfterVersion,
            IReadonlyStreamStore readonlyStreamStore,
            IObservable<Unit> streamStoreAppendedNotification,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            string name = null)
        {
            StreamId = streamId;
            _continueAfterVersion = continueAfterVersion;
            _readonlyStreamStore = readonlyStreamStore;
            _streamMessageReceived = streamMessageReceived;
            _subscriptionDropped = subscriptionDropped ?? ((_, __, ___) => { });
            _hasCaughtUp = hasCaughtUp ?? ((_) => { });
            Name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;

            _notification = streamStoreAppendedNotification.Subscribe(_ =>
            {
                _streamStoreNotification.Set();
            });

            Task.Run(PullAndPush);

            s_logger.Info($"Stream subscription created {Name} continuing after version " +
                          $"{continueAfterVersion?.ToString() ?? "<null>"}.");
        }

        public string Name { get; }

        public string StreamId { get; }

        public int? LastVersion { get; private set; }

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

        private async Task PullAndPush()
        {
            if (!_continueAfterVersion.HasValue)
            {
                _nextVersion = 0;
            }
            else if (_continueAfterVersion.Value == StreamVersion.End)
            {
                await Initialize();
            }
            else
            {
                _nextVersion = _continueAfterVersion.Value + 1;
            }

            _started.SetResult();
            while(true)
            {
                bool pause = false;
                bool? lastHasCaughtUp = null;

                while (!pause)
                {
                    var page = await Pull();

                    if (page.Status != PageReadStatus.Success)
                    {
                        break;
                    }

                    await Push(page);

                    if (!lastHasCaughtUp.HasValue || lastHasCaughtUp.Value != page.IsEnd)
                    {
                        // Only raise if the state changes
                        lastHasCaughtUp = page.IsEnd;
                        _hasCaughtUp(page.IsEnd);
                    }
                    _hasCaughtUp(page.IsEnd);

                    pause = page.IsEnd && page.Messages.Length == 0;
                }

                // Wait for notification before starting again. 
                try
                {
                    await _streamStoreNotification.WaitAsync(_disposed.Token).NotOnCapturedContext();
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
            ReadStreamPage eventsPage;
            try
            {
                // Get the last stream version and subscribe from there.
                eventsPage = await _readonlyStreamStore.ReadStreamBackwards(
                    StreamId,
                    StreamVersion.End,
                    1,
                    _disposed.Token).NotOnCapturedContext();
            }
            catch (OperationCanceledException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch (Exception ex)
            {
                s_logger.ErrorException($"Error reading stream {Name}/{StreamId}", ex);
                NotifySubscriptionDropped(SubscriptionDroppedReason.StreamStoreError, ex);
                throw;
            }

            //Only new Messages, i.e. the one after the current last one 
            _nextVersion = eventsPage.LastStreamVersion + 1;
            LastVersion = _nextVersion;
        }

        private async Task<ReadStreamPage> Pull()
        {
            ReadStreamPage readStreamPage;
            try
            {
                readStreamPage = await _readonlyStreamStore
                    .ReadStreamForwards(
                        StreamId,
                        _nextVersion,
                        MaxCountPerRead,
                        _disposed.Token)
                    .NotOnCapturedContext();
            }
            catch (OperationCanceledException)
            {
                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
                throw;
            }
            catch (Exception ex)
            {
                s_logger.ErrorException($"Error reading stream {Name}/{StreamId}", ex);
                NotifySubscriptionDropped(SubscriptionDroppedReason.StreamStoreError, ex);
                throw;
            }
            return readStreamPage;
        }

        private async Task Push(ReadStreamPage page)
        {
            foreach (var message in page.Messages)
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
                    await _streamMessageReceived(this, message).NotOnCapturedContext();
                }
                catch (Exception ex)
                {
                    s_logger.ErrorException(
                        $"Exception with subscriber receiving message {Name}/{StreamId}" +
                        $"Message: {message}.",
                        ex);
                    NotifySubscriptionDropped(SubscriptionDroppedReason.SubscriberError, ex);
                    throw;
                }
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
                s_logger.InfoException($"Subscription dropped {Name}/{StreamId}. Reason: {reason}", exception);
                _subscriptionDropped.Invoke(this, reason, exception);
            }
            catch(Exception ex)
            {
                s_logger.ErrorException(
                    $"Error notifying subscriber that subscription has been dropped ({Name}/{StreamId}).",
                    ex);
            }
        }
    }
}