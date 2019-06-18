namespace SqlStreamStore.V1.Infrastructure
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.V1.Imports.Ensure.That;

    internal class MetadataMaxAgeCache
    {
        private readonly IReadonlyStreamStore _store;
        private readonly TimeSpan _expiration;
        private readonly int _maxSize;
        private readonly GetUtcNow _getUtcNow;
        private readonly ConcurrentDictionary<string, MaxAgeCacheItem> _byStreamId 
            = new ConcurrentDictionary<string, MaxAgeCacheItem>();
        private readonly ConcurrentQueue<MaxAgeCacheItem> _byCacheStamp = new ConcurrentQueue<MaxAgeCacheItem>();
        private long _cacheHitCount;

        public MetadataMaxAgeCache(IReadonlyStreamStore store, TimeSpan expiration, int maxSize, GetUtcNow getUtcNow)
        {
            Ensure.That(store, nameof(store)).IsNotNull();
            Ensure.That(maxSize).IsGte(0);
            Ensure.That(getUtcNow).IsNotNull();

            _store = store;
            _expiration = expiration;
            _maxSize = maxSize;
            _getUtcNow = getUtcNow;
        }

        public long CacheHitCount => Interlocked.Read(ref _cacheHitCount);

        public int Count => _byStreamId.Count;

        public async Task<int?> GetMaxAge(
            string streamId,
            CancellationToken cancellationToken = default)
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

            var result = await _store.GetStreamMetadata(streamId, cancellationToken);

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