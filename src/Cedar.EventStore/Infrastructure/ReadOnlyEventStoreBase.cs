namespace Cedar.EventStore.Infrastructure
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Logging;
    using Cedar.EventStore.Streams;
    using Cedar.EventStore.Subscriptions;
    using EnsureThat;

    public abstract class ReadOnlyEventStoreBase : IReadOnlyEventStore
    {
        protected readonly GetUtcNow GetUtcNow;
        protected readonly ILog Logger;
        private bool _isDisposed;
        private readonly MetadataMaxAgeCache _metadataMaxAgeCache;

        protected ReadOnlyEventStoreBase(
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

        public async Task<AllEventsPage> ReadAllForwards(
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

        public async Task<AllEventsPage> ReadAllBackwards(
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

        public async Task<StreamEventsPage> ReadStreamForwards(
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

        public async Task<StreamEventsPage> ReadStreamBackwards(
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
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(streamEventReceived, nameof(streamEventReceived)).IsNotNull();

            CheckIfDisposed();

            return SubscribeToStreamInternal(streamId,
                fromVersionExclusive,
                streamEventReceived,
                subscriptionDropped,
                name,
                cancellationToken);
        }

        public Task<IAllStreamSubscription> SubscribeToAll(
            long? fromCheckpointExclusive,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamEventReceived, nameof(streamEventReceived)).IsNotNull();

            CheckIfDisposed();

            return SubscribeToAllInternal(fromCheckpointExclusive,
                streamEventReceived,
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

        protected abstract Task<AllEventsPage> ReadAllForwardsInternal(
            long fromCheckpointExlusive,
            int maxCount,
            CancellationToken cancellationToken);

        protected abstract Task<AllEventsPage> ReadAllBackwardsInternal(
            long fromCheckpointExclusive,
            int maxCount,
            CancellationToken cancellationToken);

        protected abstract Task<StreamEventsPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            CancellationToken cancellationToken);

        protected abstract Task<StreamEventsPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            CancellationToken cancellationToken);

        protected abstract Task<long> ReadHeadCheckpointInternal(CancellationToken cancellationToken);

        protected abstract Task<IStreamSubscription> SubscribeToStreamInternal(
            string streamId,
            int startVersion,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped,
            string name,
            CancellationToken cancellationToken);

        protected abstract Task<IAllStreamSubscription> SubscribeToAllInternal(
            long? fromCheckpoint,
            StreamEventReceived streamEventReceived,
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

        protected virtual void PurgeExpiredEvent(StreamEvent streamEvent)
        {
            //This is a no-op as this class is ReadOnly.
        }

        private async Task<StreamEventsPage> FilterExpired(
            StreamEventsPage page,
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
            var valid = new List<StreamEvent>();
            foreach(var streamEvent in page.Events)
            {
                if(streamEvent.Created.AddSeconds(maxAge.Value) > currentUtc)
                {
                    valid.Add(streamEvent);
                }
                else
                {
                    PurgeExpiredEvent(streamEvent);
                }
            }
            return new StreamEventsPage(
                page.StreamId,
                page.Status,
                page.FromStreamVersion,
                page.NextStreamVersion,
                page.LastStreamVersion,
                page.ReadDirection,
                page.IsEndOfStream,
                valid.ToArray());
        }

        private async Task<AllEventsPage> FilterExpired(
           AllEventsPage page,
           CancellationToken cancellationToken)
        {
            var valid = new List<StreamEvent>();
            var currentUtc = GetUtcNow().DateTime;
            foreach (var streamEvent in page.StreamEvents)
            {
                if(streamEvent.StreamId.StartsWith("$"))
                {
                    valid.Add(streamEvent);
                    continue;
                }
                var maxAge = await _metadataMaxAgeCache.GetMaxAge(streamEvent.StreamId, cancellationToken);
                if (!maxAge.HasValue)
                {
                    valid.Add(streamEvent);
                    continue;
                }
                if (streamEvent.Created.AddSeconds(maxAge.Value) > currentUtc)
                {
                    valid.Add(streamEvent);
                }
                else
                {
                    PurgeExpiredEvent(streamEvent);
                }
            }
            return new AllEventsPage(
                page.FromCheckpoint,
                page.NextCheckpoint,
                page.IsEnd,
                page.Direction,
                valid.ToArray());
        }

        ~ReadOnlyEventStoreBase()
        {
            Dispose(false);
        }
    }
}