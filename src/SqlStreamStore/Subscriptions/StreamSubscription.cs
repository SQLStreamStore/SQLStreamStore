namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using SqlStreamStore;
    using SqlStreamStore.Logging;

    public sealed class StreamSubscription : SubscriptionBase, IStreamSubscription
    {
        private readonly string _streamId;
        private int _nextVersion;
        private int _lastVersion;

        public StreamSubscription(
            string streamId,
            int startVersion,
            IReadonlyStreamStore readonlyStreamStore,
            IObservable<Unit> streamStoreAppendedNotification,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name = null)
            :base(readonlyStreamStore, streamStoreAppendedNotification, streamMessageReceived, subscriptionDropped, name)
        {
            _streamId = streamId;
            _nextVersion = startVersion;
            _lastVersion = startVersion - 1;

            Logger.Info($"Stream subscription {Name}/{_streamId} will start from {startVersion}.");
        }

        public string StreamId => _streamId;

        public int LastVersion => _lastVersion;

        public override async Task Start(CancellationToken cancellationToken)
        {
            if(_nextVersion == StreamVersion.End)
            {
                // Get the last stream version and subscribe from there.
                var eventsPage = await ReadonlyStreamStore.ReadStreamBackwards(
                    _streamId,
                    StreamVersion.End,
                    1,
                    cancellationToken).NotOnCapturedContext();

                //Only new Messages, i.e. the one after the current last one 
                _nextVersion = eventsPage.LastStreamVersion + 1;
            }
            await base.Start(cancellationToken);
        }

        protected override async Task<bool> DoFetch()
        {
            var streamMessagesPage = await ReadonlyStreamStore
                .ReadStreamForwards(
                    _streamId,
                    _nextVersion,
                    PageSize,
                    IsDisposed)
                .NotOnCapturedContext();
            bool isEnd = streamMessagesPage.IsEndOfStream;

            foreach(var message in streamMessagesPage.Messages)
            {
                if(IsDisposed.IsCancellationRequested)
                {
                    Logger.Warn($"Cancellation requested for stream subscription {Name}/{_streamId}. No events will be received.");
                    return true;
                }
                _nextVersion = message.StreamVersion + 1;
                _lastVersion = message.StreamVersion;
                try
                {
                    await StreamMessageReceived(message).NotOnCapturedContext();
                }
                catch(Exception ex)
                {
                    Logger.ErrorException($"Stream subscription {Name}/{_streamId} could not receive event: {message}.", ex);
                    try
                    {
                        SubscriptionDropped.Invoke(ex.Message, ex);
                    }
                    catch(Exception iex)
                    {
                        Logger.FatalException($"Tried to drop stream subscription {Name}/{_streamId} but could not.", iex);
                    }
                    finally
                    {
                        Dispose();
                    }
                }
            }
            return isEnd;
        }
    }
}
