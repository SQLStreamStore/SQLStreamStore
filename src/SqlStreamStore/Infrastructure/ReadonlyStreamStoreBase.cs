namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using SqlStreamStore.Logging;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;
    using SqlStreamStore;

    public abstract class ReadonlyStreamStoreBase : IReadonlyStreamStore
    {
        protected readonly GetUtcNow GetUtcNow;
        protected readonly ILog Logger;
        private bool _isDisposed;
        private readonly MetadataMaxAgeCache _metadataMaxAgeCache;

        protected ReadonlyStreamStoreBase(
            TimeSpan metadataMaxAgeCacheExpiry,
            int metadataMaxAgeCacheMaxSize,
            GetUtcNow getUtcNow,
            string logName)
        {
            GetUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            Logger = LogProvider.GetLogger(logName);

            _metadataMaxAgeCache = new MetadataMaxAgeCache(this, metadataMaxAgeCacheExpiry,
                metadataMaxAgeCacheMaxSize, GetUtcNow);
        }

        public async Task<AllMessagesPage> ReadAllForwards(
            long fromCheckpointInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(fromCheckpointInclusive, nameof(fromCheckpointInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            var page = await ReadAllForwardsInternal(fromCheckpointInclusive, maxCount, cancellationToken);
            return await FilterExpired(page, cancellationToken);
        }

        public async Task<AllMessagesPage> ReadAllBackwards(
            long fromCheckpointInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(fromCheckpointInclusive, nameof(fromCheckpointInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            var page = await ReadAllBackwardsInternal(fromCheckpointInclusive, maxCount, cancellationToken);
            return await FilterExpired(page, cancellationToken);
        }

        public async Task<StreamMessagesPage> ReadStreamForwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            var page = await ReadStreamForwardsInternal(streamId, fromVersionInclusive, maxCount, cancellationToken);
            return await FilterExpired(page, cancellationToken);
        }

        public async Task<StreamMessagesPage> ReadStreamBackwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            var page = await ReadStreamBackwardsInternal(streamId, fromVersionInclusive, maxCount, cancellationToken);
            return await FilterExpired(page, cancellationToken);
        }

        public Task<IStreamSubscription> SubscribeToStream(
            string streamId,
            int fromVersionExclusive,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(streamMessageReceived, nameof(streamMessageReceived)).IsNotNull();

            CheckIfDisposed();

            return SubscribeToStreamInternal(streamId,
                fromVersionExclusive,
                streamMessageReceived,
                subscriptionDropped,
                name,
                cancellationToken);
        }

        public Task<IAllStreamSubscription> SubscribeToAll(
            long? fromCheckpointExclusive,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamMessageReceived, nameof(streamMessageReceived)).IsNotNull();

            CheckIfDisposed();

            return SubscribeToAllInternal(fromCheckpointExclusive,
                streamMessageReceived,
                subscriptionDropped,
                name,
                cancellationToken);
        }

        public Task<StreamMetadataResult> GetStreamMetadata(
           string streamId,
           CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            return GetStreamMetadataInternal(streamId, cancellationToken);
        }

        public Task<long> ReadHeadCheckpoint(CancellationToken cancellationToken)
        {
            CheckIfDisposed();

            return ReadHeadCheckpointInternal(cancellationToken);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            _isDisposed = true;
        }

        protected abstract Task<AllMessagesPage> ReadAllForwardsInternal(
            long fromCheckpointExlusive,
            int maxCount,
            CancellationToken cancellationToken);

        protected abstract Task<AllMessagesPage> ReadAllBackwardsInternal(
            long fromCheckpointExclusive,
            int maxCount,
            CancellationToken cancellationToken);

        protected abstract Task<StreamMessagesPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            CancellationToken cancellationToken);

        protected abstract Task<StreamMessagesPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            CancellationToken cancellationToken);

        protected abstract Task<long> ReadHeadCheckpointInternal(CancellationToken cancellationToken);

        protected abstract Task<IStreamSubscription> SubscribeToStreamInternal(
            string streamId,
            int startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name,
            CancellationToken cancellationToken);

        protected abstract Task<IAllStreamSubscription> SubscribeToAllInternal(
            long? fromCheckpoint,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name,
            CancellationToken cancellationToken);

        protected abstract Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken);

        protected virtual void Dispose(bool disposing)
        {}

        protected void CheckIfDisposed()
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        protected virtual void PurgeExpiredEvent(StreamMessage streamMessage)
        {
            //This is a no-op as this class is ReadOnly.
        }

        private async Task<StreamMessagesPage> FilterExpired(
            StreamMessagesPage page,
            CancellationToken cancellationToken)
        {
            if(page.StreamId.StartsWith("$"))
            {
                return page;
            }
            var maxAge = await _metadataMaxAgeCache.GetMaxAge(page.StreamId, cancellationToken);
            if (!maxAge.HasValue)
            {
                return page;
            }
            var currentUtc = GetUtcNow().DateTime;
            var valid = new List<StreamMessage>();
            foreach(var message in page.Messages)
            {
                if(message.Created.AddSeconds(maxAge.Value) > currentUtc)
                {
                    valid.Add(message);
                }
                else
                {
                    PurgeExpiredEvent(message);
                }
            }
            return new StreamMessagesPage(
                page.StreamId,
                page.Status,
                page.FromStreamVersion,
                page.NextStreamVersion,
                page.LastStreamVersion,
                page.ReadDirection,
                page.IsEndOfStream,
                valid.ToArray());
        }

        private async Task<AllMessagesPage> FilterExpired(
           AllMessagesPage allMessagesPage,
           CancellationToken cancellationToken)
        {
            var valid = new List<StreamMessage>();
            var currentUtc = GetUtcNow().DateTime;
            foreach (var streamMessage in allMessagesPage.Messages)
            {
                if(streamMessage.StreamId.StartsWith("$"))
                {
                    valid.Add(streamMessage);
                    continue;
                }
                var maxAge = await _metadataMaxAgeCache.GetMaxAge(streamMessage.StreamId, cancellationToken);
                if (!maxAge.HasValue)
                {
                    valid.Add(streamMessage);
                    continue;
                }
                if (streamMessage.Created.AddSeconds(maxAge.Value) > currentUtc)
                {
                    valid.Add(streamMessage);
                }
                else
                {
                    PurgeExpiredEvent(streamMessage);
                }
            }
            return new AllMessagesPage(
                allMessagesPage.FromCheckpoint,
                allMessagesPage.NextCheckpoint,
                allMessagesPage.IsEnd,
                allMessagesPage.Direction,
                valid.ToArray());
        }

        ~ReadonlyStreamStoreBase()
        {
            Dispose(false);
        }
    }
}