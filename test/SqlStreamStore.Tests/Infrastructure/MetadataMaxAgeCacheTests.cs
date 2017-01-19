namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class MetadataMaxAgeCacheTests : IDisposable
    {
        private DateTime _currentUtc;
        private readonly InMemoryStreamStore _store;
        private readonly MetadataMaxAgeCache _cache;
        private int _maxSize = 3;
        private readonly TimeSpan _expiry = TimeSpan.FromSeconds(1);

        public MetadataMaxAgeCacheTests()
        {
            _currentUtc = new DateTime(2016, 1, 1, 0, 0, 0);
            GetUtcNow getUtcNow = () => _currentUtc;
            _store = new InMemoryStreamStore(getUtcNow);
            _cache = new MetadataMaxAgeCache(_store, _expiry, _maxSize, getUtcNow);
        }

        [Fact]
        public async Task When_lookup_for_uncached_stream_with_then_should_not_hit_cache()
        {
            string streamId = "stream";
            await _store.SetStreamMetadata(streamId, ExpectedVersion.Any, 60);

            var maxAge = await _cache.GetMaxAge(streamId);

            maxAge.Value.ShouldBe(60);
            _cache.Count.ShouldBe(1);
            _cache.CacheHitCount.ShouldBe(0);
        }

        [Fact]
        public async Task When_lookup_for_cached_stream_with_then_should_hit_cache()
        {
            string streamId = "stream";
            await _store.SetStreamMetadata(streamId, ExpectedVersion.Any, 60);
            await _cache.GetMaxAge(streamId);

            var maxAge = await _cache.GetMaxAge(streamId);

            maxAge.Value.ShouldBe(60);
            _cache.Count.ShouldBe(1);
            _cache.CacheHitCount.ShouldBe(1);
        }

        [Fact]
        public async Task When_cache_reaches_max_size_then_should_purge()
        {
            for(int i = 0; i < _maxSize + 1; i++)
            {
                await _cache.GetMaxAge($"stream-{i}");
            }

            _cache.Count.ShouldBe(_maxSize);
        }

        [Fact]
        public async Task When_cache_items_expires_then_should_not_hit_cache()
        {
            string streamId = "stream";
            await _cache.GetMaxAge(streamId);
            _currentUtc = _currentUtc + _expiry;

            await _cache.GetMaxAge(streamId);

            _cache.CacheHitCount.ShouldBe(0);
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}