namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Collections.Generic;
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
    public abstract class ReadonlyStreamStoreBase<TReadAllPage> : IReadonlyStreamStore<TReadAllPage> where TReadAllPage : IReadAllPage
    {
        private const int DefaultReloadInterval = 3000;
        protected readonly GetUtcNow GetUtcNow;
        protected readonly ILog Logger;
        private bool _isDisposed;
        private readonly MetadataMaxAgeCache _metadataMaxAgeCache;
        private readonly bool _disableMetadataCache;

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

        protected ReadonlyStreamStoreBase(GetUtcNow getUtcNow, string logName)
        {
            GetUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            Logger = LogProvider.GetLogger(logName);
            _disableMetadataCache = true;
        }

        public async Task<TReadAllPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(fromPositionInclusive, nameof(fromPositionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            Logger.DebugFormat("ReadAllForwards from position {fromPositionInclusive} with max count " +
                                   "{maxCount}.", fromPositionInclusive, maxCount);

            Task<TReadAllPage> ReadNext(long nextPosition, CancellationToken ct) => ReadAllForwards(nextPosition, maxCount, prefetchJsonData, ct);

            var page = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken)
                .ConfigureAwait(false);

            var pageHandled = await HandleGap(page, fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken);

            return await FilterExpired(pageHandled, ReadNext, cancellationToken).ConfigureAwait(false);

            //// https://github.com/damianh/SqlStreamStore/issues/31
            //// Under heavy parallel load, gaps may appear in the position sequence due to sequence
            //// number reservation of in-flight transactions.
            //// Here we check if there are any gaps, and in the unlikely event there is, we delay a little bit
            //// and re-issue the read. This is expected
            //if(!page.IsEnd || page.Messages.Length <= 1)
            //{
            //    return await FilterExpired(page, ReadNext, cancellationToken).ConfigureAwait(false);
            //}

            // only short circuit now for empty pages or 'old' pages, where
            // 'old' is defined as pages whose last message is older than _positionWriteDelayThreshold ago
            //if (page.Messages.Length == 0 || ((DateTime.UtcNow - page.Messages[page.Messages.Length - 1].CreatedUtc) > TimeSpan.FromMinutes(5)))
            //    return await FilterExpired(page, ReadNext, cancellationToken).ConfigureAwait(false);


            //// TODO: FIXIT
            //// Check for gap between last page and this.
            //if (page.Messages[0].Position != fromPositionInclusive)
            //{
            //    Logger.DebugFormat("Gap detected at lower page boundary. Potentially could have lost {lostMessageCount} events if the gap is transient", page.Messages[0].Position - fromPositionInclusive);
            //    page = await HandleGap(page, fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken);
            //    //if (!page.IsEnd || page.Messages.Length == 1)
            //    //    Logger.DebugFormat("Gap detected at lower page boundary.  Potentially could have lost {lostMessageCount} events if the gap is transient", page.Messages[0].Position - fromPositionInclusive);
            //    //page = await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken);
            //}

            //// check for gap in messages collection
            //for (int i = 0; i < page.Messages.Length - 1; i++)
            //{
            //    var expectedNextPosition = page.Messages[i].Position + 1;
            //    if (expectedNextPosition != page.Messages[i + 1].Position)
            //    {
            //        Logger.InfoFormat("Gap detected in " + (page.IsEnd ? "last" : "(NOT the last)") + " page.  Returning partial page {fromPosition}-{toPosition}", fromPositionInclusive, fromPositionInclusive + i + 1);

            //        ReadAllPage requeryPage;
            //        var maxPosition = page.Messages[page.Messages.Length - 1].Position;
            //        do
            //        {
            //            requeryPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken, maxPosition);
            //        } while (page.TxSnapshot.CurrentTxIds.Intersect(requeryPage.TxSnapshot.CurrentTxIds).Any());

            //        return await FilterExpired(requeryPage, ReadNext, cancellationToken).ConfigureAwait(false);

            //        // switched this to return the partial page, then re-issue load starting at gap
            //        // this speeds up the retry instead of taking a 3 second delay immediately
            //        //var messagesBeforeGap = new StreamMessage[i+1];
            //        //page.Messages.Take(i+1).ToArray().CopyTo(messagesBeforeGap, 0);
            //        //return new ReadAllPage(page.FromPosition, maxPosition, page.IsEnd, page.Direction, ReadNext, messagesBeforeGap);
            //    }
            //}

            //return await FilterExpired(page, ReadNext, cancellationToken).ConfigureAwait(false);
        }

        //protected virtual async Task<T> HandleGap<T>(T page, long fromPositionInclusive, int maxCount, bool prefetchJsonData, ReadNextAllPage readNext, CancellationToken cancellationToken) where T : ReadAllPage
        //{
        //    if (page.Messages.Length == 0 || DateTime.UtcNow - page.Messages[page.Messages.Length - 1].CreatedUtc > TimeSpan.FromMinutes(5))
        //        return page;


        //    // TODO: FIXIT
        //    // Check for gap between last page and this.
        //    if (page.Messages[0].Position != fromPositionInclusive)
        //    {
        //        Logger.DebugFormat("Gap detected at lower page boundary. Potentially could have lost {lostMessageCount} events if the gap is transient", page.Messages[0].Position - fromPositionInclusive);
        //        page = await HandleGap(page, fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken);
        //        //if (!page.IsEnd || page.Messages.Length == 1)
        //        //    Logger.DebugFormat("Gap detected at lower page boundary.  Potentially could have lost {lostMessageCount} events if the gap is transient", page.Messages[0].Position - fromPositionInclusive);
        //        //page = await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken);
        //    }

        //    // check for gap in messages collection
        //    for (int i = 0; i < page.Messages.Length - 1; i++)
        //    {
        //        var expectedNextPosition = page.Messages[i].Position + 1;
        //        if (expectedNextPosition != page.Messages[i + 1].Position)
        //        {
        //            Logger.InfoFormat("Gap detected in " + (page.IsEnd ? "last" : "(NOT the last)") + " page.  Returning partial page {fromPosition}-{toPosition}", fromPositionInclusive, fromPositionInclusive + i + 1);

        //            ReadAllPage requeryPage;
        //            var maxPosition = page.Messages[page.Messages.Length - 1].Position;
        //            do
        //            {
        //                requeryPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken, maxPosition);
        //            } while (page.TxSnapshot.CurrentTxIds.Intersect(requeryPage.TxSnapshot.CurrentTxIds).Any());

        //            return requeryPage;

        //            // switched this to return the partial page, then re-issue load starting at gap
        //            // this speeds up the retry instead of taking a 3 second delay immediately
        //            //var messagesBeforeGap = new StreamMessage[i+1];
        //            //page.Messages.Take(i+1).ToArray().CopyTo(messagesBeforeGap, 0);
        //            //return new ReadAllPage(page.FromPosition, maxPosition, page.IsEnd, page.Direction, ReadNext, messagesBeforeGap);
        //        }
        //    }

        //    //ReadAllPage requeryPage;
        //    //var maxPosition = pageWithGap.Messages[pageWithGap.Messages.Length - 1].Position;
        //    //do
        //    //{
        //    //    requeryPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken, maxPosition);
        //    //} while (pageWithGap.TxSnapshot.CurrentTxIds.Intersect(requeryPage.TxSnapshot.CurrentTxIds).Any());

        //    return page;
        //}

        protected virtual async Task<TReadAllPage> HandleGap(TReadAllPage page, long fromPositionInclusive, int maxCount, bool prefetchJsonData, ReadNextAllPage<TReadAllPage> readNext, CancellationToken cancellationToken)
        {
            // https://github.com/damianh/SqlStreamStore/issues/31
            // Under heavy parallel load, gaps may appear in the position sequence due to sequence
            // number reservation of in-flight transactions.
            // Here we check if there are any gaps, and in the unlikely event there is, we delay a little bit
            // and re-issue the read. This is expected
            if(!page.IsEnd || page.Messages.Length <= 1)
            {
                return page;
            }

            // Check for gap between last page and this.
            if (page.Messages[0].Position != fromPositionInclusive)
            {
                return await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken);
            }

            // check for gap in messages collection
            for(int i = 0; i < page.Messages.Length - 1; i++)
            {
                if(page.Messages[i].Position + 1 != page.Messages[i + 1].Position)
                {
                    return await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken);
                }
            }

            return page;
        }

        public async Task<TReadAllPage> ReadAllBackwards(
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

            ReadNextAllPage<TReadAllPage> readNext = (nextPosition, ct) => ReadAllBackwards(nextPosition, maxCount, prefetchJsonData, ct);
            var page = await ReadAllBackwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
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
            var page = await ReadStreamForwardsInternal(streamId, fromVersionInclusive, maxCount, prefetchJsonData,
                readNext, cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
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
            var page = await ReadStreamBackwardsInternal(streamId, fromVersionInclusive, maxCount, prefetchJsonData, readNext,
                cancellationToken);
            return await FilterExpired(page, readNext, cancellationToken);
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
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            if (streamId.StartsWith("$") && streamId != Deleted.DeletedStreamId)
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
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));

            GuardAgainstDisposed();

            return ReadStreamHeadPositionInternal(streamId, cancellationToken);
        }

        public Task<int> ReadStreamHeadVersion(StreamId streamId, CancellationToken cancellationToken = default)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));

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

        protected abstract Task<TReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage<TReadAllPage> readNext,
            CancellationToken cancellationToken,
            long fromMaxPositionInclusive = -1);

        protected abstract Task<TReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage<TReadAllPage> readNext,
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
            if (_isDisposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        protected abstract void PurgeExpiredMessage(StreamMessage streamMessage);

        private async Task<TReadAllPage> ReloadAfterDelay(
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage<TReadAllPage> readNext,
            CancellationToken cancellationToken)
        {
            Logger.Info("ReadAllForwards: gap detected in position, reloading after {DefaultReloadInterval}ms, position: {fromPositionInclusive}", DefaultReloadInterval, fromPositionInclusive);
            //await Task.Delay(DefaultReloadInterval, cancellationToken);
            var reloadedPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetch, readNext, cancellationToken)
                .ConfigureAwait(false);
            return await FilterExpired(reloadedPage, readNext, cancellationToken).ConfigureAwait(false);
        }

        private async Task<ReadStreamPage> FilterExpired(
            ReadStreamPage page,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            if (page.StreamId.StartsWith("$"))
            {
                return page;
            }

            int? maxAge = _metadataMaxAgeCache == null
                ? null
                : await _metadataMaxAgeCache.GetMaxAge(page.StreamId, cancellationToken);
            if (!maxAge.HasValue)
            {
                return page;
            }
            var currentUtc = GetUtcNow();
            var valid = new List<StreamMessage>();
            foreach (var message in page.Messages)
            {
                if (message.CreatedUtc.AddSeconds(maxAge.Value) > currentUtc)
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

        private async Task<TReadAllPage> FilterExpired(TReadAllPage readAllPage,
           ReadNextAllPage<TReadAllPage> readNext,
           CancellationToken cancellationToken)
        {
            return readAllPage;
            if (_disableMetadataCache)
            {
                return readAllPage;
            }
            var valid = new List<StreamMessage>();
            var currentUtc = GetUtcNow();
            foreach (var streamMessage in readAllPage.Messages)
            {
                if (streamMessage.StreamId.StartsWith("$"))
                {
                    valid.Add(streamMessage);
                    continue;
                }
                int? maxAge = _metadataMaxAgeCache == null
                    ? null
                    : await _metadataMaxAgeCache.GetMaxAge(streamMessage.StreamId, cancellationToken);
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
            //return new ReadAllPage(
            //    readAllPage.FromPosition,
            //    readAllPage.NextPosition,
            //    readAllPage.IsEnd,
            //    readAllPage.Direction,
            //    readNext,
            //    valid.ToArray());
        }

        protected List<StreamMessage> FilterExpired(List<(StreamMessage StreamMessage, int? MaxAge)> messages)
        {
            var valid = new List<StreamMessage>();
            var currentUtc = GetUtcNow();
            foreach (var item in messages)
            {
                if (item.StreamMessage.StreamId.StartsWith("$"))
                {
                    valid.Add(item.StreamMessage);
                    continue;
                }
                if (!item.MaxAge.HasValue || item.MaxAge <= 0)
                {
                    valid.Add(item.StreamMessage);
                    continue;
                }
                if (item.StreamMessage.CreatedUtc.AddSeconds(item.MaxAge.Value) > currentUtc)
                {
                    valid.Add(item.StreamMessage);
                }
                else
                {
                    PurgeExpiredMessage(item.StreamMessage);
                }
            }
            return valid;
        }

        ~ReadonlyStreamStoreBase()
        {
            Dispose(false);
        }
    }
}
