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
        private const int DefaultReloadInterval = 3000;
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

        public async Task<ReadAllPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
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

            ReadNextAllPage readNext = (nextPosition, ct) => ReadAllForwards(nextPosition, maxCount, ct);

            var page = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, readNext, cancellationToken)
                .NotOnCapturedContext();

            // https://github.com/damianh/SqlStreamStore/issues/31
            // Under heavy parallel load, gaps may appear in the position sequence due to sequence
            // number reservation of in-flight transactions.
            // Here we check if there are any gaps, and in the unlikely event there is, we delay a little bit
            // and re-issue the read. This is expected 
            if(!page.IsEnd || page.Messages.Length <= 1)
            {
                return await FilterExpired(page, readNext, cancellationToken).NotOnCapturedContext();
            }

            // Check for gap between last page and this.
            if (page.Messages[0].Position != fromPositionInclusive)
            {
                page = await ReloadAfterDelay(fromPositionInclusive, maxCount, readNext, cancellationToken);
            }
            
            // check for gap in messages collection
            for(int i = 0; i < page.Messages.Length - 1; i++)
            {
                if(page.Messages[i].Position + 1 != page.Messages[i + 1].Position)
                {
                    page = await ReloadAfterDelay(fromPositionInclusive, maxCount, readNext, cancellationToken);
                    break;
                }
            }

            return await FilterExpired(page, readNext, cancellationToken).NotOnCapturedContext();
        }

        public async Task<ReadAllPage> ReadAllBackwards(
            long fromPositionInclusive,
            int maxCount,
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

            ReadNextAllPage readNext = (nextPosition, ct) => ReadAllBackwards(nextPosition, maxCount, ct);
            var page = await ReadAllBackwardsInternal(fromPositionInclusive, maxCount, readNext, cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
        }

        public async Task<ReadStreamPage> ReadStreamForwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadStreamForwards {streamId} from version {fromVersionInclusive} with max count " +
                                   "{maxCount}.", streamId, fromVersionInclusive, maxCount);
            }

            ReadNextStreamPage readNext = (nextVersion, ct) => ReadStreamForwards(streamId, nextVersion, maxCount, ct);
            var page = await ReadStreamForwardsInternal(streamId, fromVersionInclusive, maxCount, readNext, cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
        }

        public async Task<ReadStreamPage> ReadStreamBackwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadStreamBackwards {streamId} from version {fromVersionInclusive} with max count " +
                                   "{maxCount}.", streamId, fromVersionInclusive, maxCount);
            }
            ReadNextStreamPage readNext = (nextVersion, ct) => ReadStreamBackwards(streamId, nextVersion, maxCount, ct);
            var page = await ReadStreamBackwardsInternal(streamId, fromVersionInclusive, maxCount, readNext, cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
        }

        public IStreamSubscription SubscribeToStream(
            string streamId,
            int? continueAfterVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            string name = null)
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(streamMessageReceived, nameof(streamMessageReceived)).IsNotNull();

            GuardAgainstDisposed();
            
            return SubscribeToStreamInternal(
                streamId,
                continueAfterVersion,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                name);
        }

        public IAllStreamSubscription SubscribeToAll(
            long? continueAfterPosition,
            AllStreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            string name = null)
        {
            Ensure.That(streamMessageReceived, nameof(streamMessageReceived)).IsNotNull();

            GuardAgainstDisposed();

            return SubscribeToAllInternal(continueAfterPosition,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                name);
        }

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

        public Task<long> ReadHeadPosition(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            return ReadHeadPositionInternal(cancellationToken);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            _isDisposed = true;
        }

        protected abstract Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExlusive,
            int maxCount,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken);

        protected abstract Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken);

        protected abstract Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken);

        protected abstract Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken);

        protected abstract Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken);

        protected abstract IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            string name);

        protected abstract IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            AllStreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
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

        private async Task<ReadAllPage> ReloadAfterDelay(long fromPositionInclusive, int maxCount,
            ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            Logger.InfoFormat($"ReadAllForwards: gap detected in position, reloading after {DefaultReloadInterval}ms");
            await Task.Delay(DefaultReloadInterval, cancellationToken);
            var reloadedPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, readNext, cancellationToken)
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