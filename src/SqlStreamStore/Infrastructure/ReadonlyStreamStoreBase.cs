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

        public async Task<AllMessagesPage> ReadAllForwards(
            long fromPosition,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(fromPosition, nameof(fromPosition)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadAllForwards from position {fromPositionInclusive} with max count " +
                                   "{maxCount}.", fromPosition, maxCount);
            }

            var page = await ReadAllForwardsInternal(fromPosition, maxCount, cancellationToken)
                .NotOnCapturedContext();

            // https://github.com/damianh/SqlStreamStore/issues/31
            // Under heavy parallel load, gaps may appear in the position sequence due to sequence
            // number reservation of in-flight transactions.
            // Here we check if there are any gaps, and in the unlikely event there is, we delay a little bit
            // and re-issue the read. This is expected 
            if(!page.IsEnd || page.Messages.Length <= 1)
            {
                return await FilterExpired(page, cancellationToken).NotOnCapturedContext();
            }

            Func<CancellationToken, Task<AllMessagesPage>> reload = async ct =>
            {
                Logger.InfoFormat($"ReadAllForwards: gap detected in position, reloading after {DefaultReloadInterval}ms");
                await Task.Delay(DefaultReloadInterval, cancellationToken);
                var reloadedPage = await ReadAllForwardsInternal(fromPosition, maxCount, cancellationToken)
                    .NotOnCapturedContext();
                return await FilterExpired(reloadedPage, cancellationToken).NotOnCapturedContext();
            };

            // Check for gap between last page and this.
            if (page.Messages[0].Position != fromPosition)
            {
                return await reload(cancellationToken);
            }
            
            // check for gap in messages collection
            for(int i = 0; i < page.Messages.Length - 1; i++)
            {
                if(page.Messages[i].Position + 1 != page.Messages[i + 1].Position)
                {
                    return await reload(cancellationToken);
                }
            }

            return await FilterExpired(page, cancellationToken).NotOnCapturedContext();
        }

        public async Task<AllMessagesPage> ReadAllBackwards(
            long fromPosition,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(fromPosition, nameof(fromPosition)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadAllBackwards from position {fromPositionInclusive} with max count " +
                                   "{maxCount}.", fromPosition, maxCount);
            }

            var page = await ReadAllBackwardsInternal(fromPosition, maxCount, cancellationToken);
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

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadStreamForwards {streamId} from version {fromVersionInclusive} with max count " +
                                   "{maxCount}.", streamId, fromVersionInclusive, maxCount);
            }

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

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("ReadStreamBackwards {streamId} from version {fromVersionInclusive} with max count " +
                                   "{maxCount}.", streamId, fromVersionInclusive, maxCount);
            }

            var page = await ReadStreamBackwardsInternal(streamId, fromVersionInclusive, maxCount, cancellationToken);
            return await FilterExpired(page, cancellationToken);
        }

        public IStreamSubscription SubscribeToStream(
            string streamId,
            int? contiuneAfterVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null)
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(streamMessageReceived, nameof(streamMessageReceived)).IsNotNull();

            CheckIfDisposed();

            return SubscribeToStreamInternal(
                streamId,
                contiuneAfterVersion,
                streamMessageReceived,
                subscriptionDropped,
                name);
        }

        public IAllStreamSubscription SubscribeToAll(
            long? continueAfterPosition,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null)
        {
            Ensure.That(streamMessageReceived, nameof(streamMessageReceived)).IsNotNull();

            CheckIfDisposed();

            return SubscribeToAllInternal(
                continueAfterPosition,
                streamMessageReceived,
                subscriptionDropped,
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
            CheckIfDisposed();

            return ReadHeadPositionInternal(cancellationToken);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            _isDisposed = true;
        }

        protected abstract Task<AllMessagesPage> ReadAllForwardsInternal(
            long fromPositionExlusive,
            int maxCount,
            CancellationToken cancellationToken);

        protected abstract Task<AllMessagesPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
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

        protected abstract Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken);

        protected abstract IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? continueAfterVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name);

        protected abstract IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name);

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

        protected abstract void PurgeExpiredMessage(StreamMessage streamMessage);

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
            var currentUtc = GetUtcNow();
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
                if (streamMessage.CreatedUtc.AddSeconds(maxAge.Value) > currentUtc)
                {
                    valid.Add(streamMessage);
                }
                else
                {
                    PurgeExpiredMessage(streamMessage);
                }
            }
            return new AllMessagesPage(
                allMessagesPage.FromPosition,
                allMessagesPage.NextPosition,
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