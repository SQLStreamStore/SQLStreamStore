namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Logging;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;
    using SqlStreamStore;
    using SqlStreamStore.Imports.Ensure.That;

    /// <summary>
    ///     Represents a base implementation of a readonly stream store.
    /// </summary>
    public abstract class ReadonlyStreamStoreBase : IReadonlyStreamStore
    {
        private const int DefaultReloadInterval = 3000;
        private readonly GetUtcNow _getUtcNow;
        protected readonly ILog Logger;
        private bool _isDisposed;
        private readonly MetadataMaxAgeCache _metadataMaxAgeCache;
        private readonly bool _disableMetadataCache;
        private readonly GapHandlingSettings _gapHandlingSettings;

        protected ReadonlyStreamStoreBase(
            TimeSpan metadataMaxAgeCacheExpiry,
            int metadataMaxAgeCacheMaxSize,
            GetUtcNow getUtcNow,
            string logName,
            GapHandlingSettings gapHandlingSettings = null)
        {
            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            Logger = LogProvider.GetLogger(logName);

            _metadataMaxAgeCache = new MetadataMaxAgeCache(this,
                metadataMaxAgeCacheExpiry,
                metadataMaxAgeCacheMaxSize,
                _getUtcNow);

            _gapHandlingSettings = gapHandlingSettings;
        }

        protected ReadonlyStreamStoreBase(GetUtcNow getUtcNow, string logName, GapHandlingSettings gapHandlingSettings = null)
        {
            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            Logger = LogProvider.GetLogger(logName);
            _disableMetadataCache = true;
            _gapHandlingSettings = gapHandlingSettings;
        }

        public async Task<ReadAllPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromPositionInclusive, nameof(fromPositionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            Logger.DebugFormat("ReadAllForwards from position {fromPositionInclusive} with max count {maxCount}.",
                fromPositionInclusive,
                maxCount);

            Task<ReadAllPage> ReadNext(long nextPosition, CancellationToken ct) => ReadAllForwards(nextPosition, maxCount, prefetchJsonData, ct);

            var page = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken)
                .ConfigureAwait(false);

            if(_gapHandlingSettings != null)
            {
                // Gaps are handled on a lower level
                return await FilterExpired(page, ReadNext, cancellationToken).ConfigureAwait(false);
            }

            // https://github.com/damianh/SqlStreamStore/issues/31
            // Under heavy parallel load, gaps may appear in the position sequence due to sequence
            // number reservation of in-flight transactions.
            // Here we check if there are any gaps, and in the unlikely event there is, we delay a little bit
            // and re-issue the read. This is expected
            if(!page.IsEnd || page.Messages.Length <= 1)
            {
                return await FilterExpired(page, ReadNext, cancellationToken).ConfigureAwait(false);
            }

            // Check for gap between last page and this.
            if(page.Messages[0].Position != fromPositionInclusive)
            {
                page = await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken).ConfigureAwait(false);
            }

            // check for gap in messages collection
            for(int i = 0; i < page.Messages.Length - 1; i++)
            {
                if(page.Messages[i].Position + 1 == page.Messages[i + 1].Position)
                    continue;
                page = await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken).ConfigureAwait(false);
                break;
            }

            return await FilterExpired(page, ReadNext, cancellationToken).ConfigureAwait(false);
        }

        public async Task<ReadAllPage> ReadAllBackwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromPositionInclusive, nameof(fromPositionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            Logger.Debug(
                "ReadAllBackwards from position {fromPositionInclusive} with max count {maxCount}.",
                fromPositionInclusive,
                maxCount);

            ReadNextAllPage readNext = (nextPosition, ct) => ReadAllBackwards(nextPosition, maxCount, prefetchJsonData, ct);
            var page = await ReadAllBackwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken).ConfigureAwait(false);
            return await FilterExpired(page, readNext, cancellationToken).ConfigureAwait(false);
        }

        public async Task<ReadStreamPage> ReadStreamForwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            Logger.Debug(
                "ReadStreamForwards {streamId} from version {fromVersionInclusive} with max count {maxCount}.",
                streamId,
                fromVersionInclusive,
                maxCount);

            ReadNextStreamPage readNext = (nextVersion, ct) => ReadStreamForwards(streamId, nextVersion, maxCount, prefetchJsonData, ct);
            var page = await ReadStreamForwardsInternal(streamId,
                fromVersionInclusive,
                maxCount,
                prefetchJsonData,
                readNext,
                cancellationToken).ConfigureAwait(false);
            return await FilterExpired(page, readNext, cancellationToken).ConfigureAwait(false);
        }

        public async Task<ReadStreamPage> ReadStreamBackwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            Logger.Debug(
                "ReadStreamBackwards {streamId} from version {fromVersionInclusive} with max count {maxCount}.",
                streamId,
                fromVersionInclusive,
                maxCount);

            ReadNextStreamPage readNext =
                (nextVersion, ct) => ReadStreamBackwards(streamId, nextVersion, maxCount, prefetchJsonData, ct);
            var page = await ReadStreamBackwardsInternal(streamId,
                fromVersionInclusive,
                maxCount,
                prefetchJsonData,
                readNext,
                cancellationToken).ConfigureAwait(false);
            return await FilterExpired(page, readNext, cancellationToken).ConfigureAwait(false);
        }

        public IStreamSubscription SubscribeToStream(
            StreamId streamId,
            int? continueAfterVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            bool prefetchJsonData = true,
            string name = null)
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNull();
            Ensure.That(streamMessageReceived, nameof(streamMessageReceived)).IsNotNull();

            GuardAgainstDisposed();

            return SubscribeToStreamInternal(
                streamId,
                continueAfterVersion,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);
        }

        public IAllStreamSubscription SubscribeToAll(
            long? continueAfterPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            bool prefetchJsonData = true,
            string name = null)
        {
            Ensure.That(streamMessageReceived, nameof(streamMessageReceived)).IsNotNull();

            GuardAgainstDisposed();

            return SubscribeToAllInternal(
                continueAfterPosition,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);
        }

        public Task<StreamMetadataResult> GetStreamMetadata(
            string streamId,
            CancellationToken cancellationToken = default)
        {
            if(streamId == null) throw new ArgumentNullException(nameof(streamId));
            if(streamId.StartsWith("$") && streamId != Deleted.DeletedStreamId)
            {
                throw new ArgumentException("Must not start with '$'", nameof(streamId));
            }

            Logger.Debug("GetStreamMetadata {streamId}.", streamId);

            return GetStreamMetadataInternal(streamId, cancellationToken);
        }

        public Task<long> ReadHeadPosition(CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            return ReadHeadPositionInternal(cancellationToken);
        }

        public Task<long> ReadStreamHeadPosition(StreamId streamId, CancellationToken cancellationToken = default)
        {
            if(streamId == null) throw new ArgumentNullException(nameof(streamId));

            GuardAgainstDisposed();

            return ReadStreamHeadPositionInternal(streamId, cancellationToken);
        }

        public Task<int> ReadStreamHeadVersion(StreamId streamId, CancellationToken cancellationToken = default)
        {
            if(streamId == null) throw new ArgumentNullException(nameof(streamId));

            GuardAgainstDisposed();

            return ReadStreamHeadVersionInternal(streamId, cancellationToken);
        }

        public Task<ListStreamsPage> ListStreams(
            int maxCount = 100,
            string continuationToken = default,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(maxCount).IsGt(0);

            GuardAgainstDisposed();

            return ListStreams(Pattern.Anything(), maxCount, continuationToken, cancellationToken);
        }

        public Task<ListStreamsPage> ListStreams(
            Pattern pattern,
            int maxCount = 100,
            string continuationToken = default,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(maxCount).IsGt(0);
            Ensure.That(pattern).IsNotNull();

            Task<ListStreamsPage> ListNext(string @continue, CancellationToken ct)
                => ListStreams(pattern, maxCount, @continue, ct);

            return ListStreamsInternal(pattern, maxCount, continuationToken, ListNext, cancellationToken);
        }

        public void Dispose()
        {
            OnDispose?.Invoke();
            Dispose(true);
            GC.SuppressFinalize(this);
            _isDisposed = true;
        }

        public event Action OnDispose;

        protected abstract Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken);

        protected abstract Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken);

        protected abstract Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken);

        protected abstract Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken);

        protected abstract Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken);
        protected abstract Task<long> ReadStreamHeadPositionInternal(string streamId, CancellationToken cancellationToken);
        protected abstract Task<int> ReadStreamHeadVersionInternal(string streamId, CancellationToken cancellationToken);

        protected abstract IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name);

        protected abstract IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name);

        protected abstract Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken);

        protected abstract Task<ListStreamsPage> ListStreamsInternal(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken);

        protected virtual void Dispose(bool disposing)
        { }

        protected void GuardAgainstDisposed()
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        protected abstract void PurgeExpiredMessage(StreamMessage streamMessage);

        private async Task<ReadAllPage> ReloadAfterDelay(
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            Logger.Info("ReadAllForwards: gap detected in position, reloading after {DefaultReloadInterval}ms", DefaultReloadInterval);
            await Task.Delay(DefaultReloadInterval, cancellationToken).ConfigureAwait(false);
            var reloadedPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetch, readNext, cancellationToken)
                .ConfigureAwait(false);
            return await FilterExpired(reloadedPage, readNext, cancellationToken).ConfigureAwait(false);
        }

        private async Task<ReadStreamPage> FilterExpired(
            ReadStreamPage page,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            if(page.StreamId.StartsWith("$"))
            {
                return page;
            }

            int? maxAge = _metadataMaxAgeCache == null
                ? null
                : await _metadataMaxAgeCache.GetMaxAge(page.StreamId, cancellationToken).ConfigureAwait(false);
            if(!maxAge.HasValue)
            {
                return page;
            }

            var currentUtc = _getUtcNow();
            var valid = new List<StreamMessage>();
            foreach(var message in page.Messages)
            {
                if(message.CreatedUtc.AddSeconds(maxAge.Value) > currentUtc)
                {
                    valid.Add(message);
                }
                else
                {
                    PurgeExpiredMessage(message);
                }
            }

            return new ReadStreamPage(
                page.StreamId,
                page.Status,
                page.FromStreamVersion,
                page.NextStreamVersion,
                page.LastStreamVersion,
                page.LastStreamPosition,
                page.ReadDirection,
                page.IsEnd,
                readNext,
                valid.ToArray());
        }

        private async Task<ReadAllPage> FilterExpired(
            ReadAllPage readAllPage,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            if(_disableMetadataCache)
            {
                return readAllPage;
            }

            var valid = new List<StreamMessage>();
            var currentUtc = _getUtcNow();
            foreach(var streamMessage in readAllPage.Messages)
            {
                if(streamMessage.StreamId.StartsWith("$"))
                {
                    valid.Add(streamMessage);
                    continue;
                }

                int? maxAge = _metadataMaxAgeCache == null
                    ? null
                    : await _metadataMaxAgeCache.GetMaxAge(streamMessage.StreamId, cancellationToken).ConfigureAwait(false);
                if(!maxAge.HasValue)
                {
                    valid.Add(streamMessage);
                    continue;
                }

                if(streamMessage.CreatedUtc.AddSeconds(maxAge.Value) > currentUtc)
                {
                    valid.Add(streamMessage);
                }
                else
                {
                    PurgeExpiredMessage(streamMessage);
                }
            }

            return new ReadAllPage(
                readAllPage.FromPosition,
                readAllPage.NextPosition,
                readAllPage.IsEnd,
                readAllPage.Direction,
                readNext,
                valid.ToArray());
        }

        protected ReadOnlyCollection<StreamMessage> FilterExpired(ReadOnlyCollection<StreamMessage> messages, ReadOnlyDictionary<string, int> maxAgeDict)
        {
            if(maxAgeDict.Count == 0)
                return messages;

            var valid = new List<StreamMessage>();
            var currentUtc = _getUtcNow();
            foreach(var message in messages)
            {
                if(message.StreamId.StartsWith("$"))
                {
                    valid.Add(message);
                    continue;
                }

                if(!maxAgeDict.TryGetValue(message.StreamId, out int maxAge) || maxAge <= 0)
                {
                    valid.Add(message);
                    continue;
                }

                if(message.CreatedUtc.AddSeconds(maxAge) > currentUtc)
                {
                    valid.Add(message);
                }
                else
                {
                    PurgeExpiredMessage(message);
                }
            }

            return valid.AsReadOnly();
        }

        ~ReadonlyStreamStoreBase()
        {
            Dispose(false);
        }
    }
}