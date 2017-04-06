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

    /// <summary>
    ///     An abstract implementation of a <see cref="IReadonlyStreamStore"/> that provides shared behaviour across
    ///     all readonly stream store implementations such as guard clauses, logging and filtering expired messages.
    /// </summary>
    public abstract class ReadonlyStreamStoreBase : IReadonlyStreamStore
    {
        private const int DefaultReloadInterval = 3000;
        protected readonly GetUtcNow GetUtcNow;
        protected readonly ILog Logger;
        private bool _isDisposed;
        private readonly MetadataMaxAgeCache _metadataMaxAgeCache;

        /// <summary>
        ///     Initialized a new instance of <see cref="ReadonlyStreamStoreBase"/>
        /// </summary>
        /// <param name="metadataMaxAgeCacheExpiry"></param>
        /// <param name="metadataMaxAgeCacheMaxSize"></param>
        /// <param name="getUtcNow"></param>
        /// <param name="logName"></param>
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

        /// <inheritdoc />
        public async Task<ReadAllPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(fromPositionInclusive, nameof(fromPositionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);
            GuardAgainstDisposed();

            cancellationToken.ThrowIfCancellationRequested();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadAllForwards from position {fromPositionInclusive} with max count " +
                                   "{maxCount}.", fromPositionInclusive, maxCount);
            }

            ReadNextAllPage readNext = (nextPosition, ct) => ReadAllForwards(nextPosition, maxCount, prefetchJsonData, ct);

            var page = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken)
                .NotOnCapturedContext();

            // https://github.com/damianh/SqlStreamStore/issues/31
            // Under heavy parallel load, gaps may appear in the position sequence due to sequence
            // number reservation of in-flight transactions.
            // Here we check if there are any gaps, and in the unlikely event there is, we delay a little bit
            // and re-issue the read. This is expected.
            if(!page.IsEnd || page.Messages.Length <= 1)
            {
                return await FilterExpired(page, readNext, cancellationToken).NotOnCapturedContext();
            }

            // Check for gap between last page and this.
            if (page.Messages[0].Position != fromPositionInclusive)
            {
                page = await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken);
            }

            // check for gap in messages collection
            for(int i = 0; i < page.Messages.Length - 1; i++)
            {
                if(page.Messages[i].Position + 1 != page.Messages[i + 1].Position)
                {
                    page = await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken);
                    break;
                }
            }

            return await FilterExpired(page, readNext, cancellationToken).NotOnCapturedContext();
        }

        /// <inheritdoc />
        public async Task<ReadAllPage> ReadAllBackwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(fromPositionInclusive, nameof(fromPositionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);
            GuardAgainstDisposed();

            cancellationToken.ThrowIfCancellationRequested();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadAllBackwards from position {fromPositionInclusive} with max count " +
                                   "{maxCount}.", fromPositionInclusive, maxCount);
            }

            ReadNextAllPage readNext = (nextPosition, ct) => ReadAllBackwards(nextPosition, maxCount, prefetchJsonData, ct);
            var page = await ReadAllBackwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<ReadStreamPage> ReadStreamForwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadStreamForwards {streamId} from version {fromVersionInclusive} with max count " +
                                   "{maxCount}.", streamId, fromVersionInclusive, maxCount);
            }

            ReadNextStreamPage readNext = (nextVersion, ct) => ReadStreamForwards(streamId, nextVersion, maxCount, prefetchJsonData, ct);
            var page = await ReadStreamForwardsInternal(streamId, fromVersionInclusive, maxCount, prefetchJsonData,
                readNext, cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<ReadStreamPage> ReadStreamBackwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadStreamBackwards {streamId} from version {fromVersionInclusive} with max count " +
                                   "{maxCount}.", streamId, fromVersionInclusive, maxCount);
            }
            ReadNextStreamPage readNext =
                (nextVersion, ct) => ReadStreamBackwards(streamId, nextVersion, maxCount, prefetchJsonData, ct);
            var page = await ReadStreamBackwardsInternal(streamId, fromVersionInclusive, maxCount, prefetchJsonData, readNext,
                cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
        }

        /// <inheritdoc />
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

        /// <inheritdoc />
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

        /// <inheritdoc />
        public Task<StreamMetadataResult> GetStreamMetadata(
           string streamId,
           CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("GetStreamMetadata {streamId}.", streamId);
            }

            return GetStreamMetadataInternal(streamId, cancellationToken);
        }

        /// <inheritdoc />
        public Task<long> ReadHeadPosition(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            return ReadHeadPositionInternal(cancellationToken);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            OnDispose?.Invoke();
            Dispose(true);
            GC.SuppressFinalize(this);
            _isDisposed = true;
        }

        public event Action OnDispose;

        protected abstract Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExlusive,
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
            ReadNextStreamPage readNext, CancellationToken cancellationToken);

        protected abstract Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken);

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

        protected virtual void Dispose(bool disposing)
        {}

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
            Logger.InfoFormat($"ReadAllForwards: gap detected in position, reloading after {DefaultReloadInterval}ms");
            await Task.Delay(DefaultReloadInterval, cancellationToken);
            var reloadedPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetch, readNext, cancellationToken)
                .NotOnCapturedContext();
            return await FilterExpired(reloadedPage, readNext, cancellationToken).NotOnCapturedContext();
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
            var maxAge = await _metadataMaxAgeCache.GetMaxAge(page.StreamId, cancellationToken);
            if (!maxAge.HasValue)
            {
                return page;
            }
            var currentUtc = GetUtcNow();
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
                page.ReadDirection,
                page.IsEnd,
                valid.ToArray(),
                readNext);
        }

        private async Task<ReadAllPage> FilterExpired(
           ReadAllPage readAllPage,
           ReadNextAllPage readNext,
           CancellationToken cancellationToken)
        {
            var valid = new List<StreamMessage>();
            var currentUtc = GetUtcNow();
            foreach (var streamMessage in readAllPage.Messages)
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
                if (streamMessage.CreatedUtc.AddSeconds(maxAge.Value) > currentUtc)
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
                valid.ToArray(),
                readNext);
        }

        ~ReadonlyStreamStoreBase()
        {
            Dispose(false);
        }
    }
}