namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using SqlStreamStore;

    /// <summary>
    ///     A cache to retain the MaxAge metadata of a stream. Helps with performance when reading and writing
    ///     to streams. The cache expiration window means that if the MaxAge metadata for a stream changes, 
    ///     it will be up to this time span before it is applied (if the value is cached).
    /// </summary>
    public class MetadataMaxAgeCache
    {
        private readonly IReadonlyStreamStore _readonlyStreamStore;
        private readonly TimeSpan _expiration;
        private readonly int _maxSize;
        private readonly GetUtcNow _getUtcNow;
        private readonly ConcurrentDictionary<string, MaxAgeCacheItem> _byStreamId 
            = new ConcurrentDictionary<string, MaxAgeCacheItem>();
        private readonly ConcurrentQueue<MaxAgeCacheItem> _byCacheStamp = new ConcurrentQueue<MaxAgeCacheItem>();
        private long _cacheHitCount;
        private long _cacheMissCount;

        /// <summary>
        ///     Initializes a new instance of <see cref="MetadataMaxAgeCache"/>
        /// </summary>
        /// <param name="readonlyStreamStore">A readonly streamstore instance.</param>
        /// <param name="expiration">The timespan for which to cache the max age meta data.</param>
        /// <param name="maxSize">The maximum number of stream max age metadata items to cache.</param>
        /// <param name="getUtcNow">A delegate to get the current UTC data time.</param>
        public MetadataMaxAgeCache(IReadonlyStreamStore readonlyStreamStore, TimeSpan expiration, int maxSize, GetUtcNow getUtcNow)
        {
            Ensure.That(readonlyStreamStore, nameof(readonlyStreamStore)).IsNotNull();
            Ensure.That(expiration).IsGte(TimeSpan.FromSeconds(1));
            Ensure.That(maxSize).IsGte(0);
            Ensure.That(getUtcNow).IsNotNull();

            _readonlyStreamStore = readonlyStreamStore;
            _expiration = expiration;
            _maxSize = maxSize;
            _getUtcNow = getUtcNow;
        }

        /// <summary>
        ///     The number of times the cache has been hit.
        /// </summary>
        public long CacheHitCount => Interlocked.Read(ref _cacheHitCount);

        /// <summary>
        ///     The number of times the cache has been missed.
        /// </summary>
        public long CacheMissCount => Interlocked.Read(ref _cacheMissCount);

        /// <summary>
        ///     The number of items cached.
        /// </summary>
        public int Count => _byStreamId.Count;

        /// <summary>
        ///     Gets the Max age of a stream. If the metadata is cached and not expired, then that is returned.
        ///     Otherwise the metadata is looked up.
        /// </summary>
        /// <param name="streamId">The stream Id to ge tthe Max Age metadata.</param>
        /// <param name="cancellationToken">An optional cancellation token to cancel the request.</param>
        /// <returns>A max age value. Null if no max age for stream is found.</returns>
        public async Task<int?> GetMaxAge(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var utcNow = _getUtcNow();
            MaxAgeCacheItem cacheItem;
            if(_byStreamId.TryGetValue(streamId, out cacheItem))
            {
                var expiresAt = cacheItem.CachedStampUtc + _expiration;
                if(expiresAt > utcNow)
                {
                    Interlocked.Increment(ref _cacheHitCount);
                    return cacheItem.MaxAge;
                }
            }

            var result = await _readonlyStreamStore.GetStreamMetadata(streamId, cancellationToken);

            cacheItem = new MaxAgeCacheItem(streamId, utcNow, result.MaxAge);
            _byStreamId.AddOrUpdate(streamId, cacheItem, (_, __) => cacheItem);
            _byCacheStamp.Enqueue(cacheItem);

            while(_byCacheStamp.Count > _maxSize)
            {
                if(_byCacheStamp.TryDequeue(out cacheItem))
                {
                    _byStreamId.TryRemove(cacheItem.StreamId, out cacheItem);
                }
            }
            Interlocked.Increment(ref _cacheMissCount);
            return result.MaxAge;
        }

        private struct MaxAgeCacheItem
        {
            internal readonly string StreamId;
            internal readonly DateTime CachedStampUtc;
            internal readonly int? MaxAge;

            internal MaxAgeCacheItem(string streamId, DateTime cachedStampUtc, int? maxAge)
            {
                StreamId = streamId;
                CachedStampUtc = cachedStampUtc;
                MaxAge = maxAge;
            }
        }
    }
}