namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public abstract class StreamStoreAcceptanceTestFixture : IDisposable
    {
        public abstract Task<IStreamStore> GetStreamStore();

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public virtual void Dispose()
        {}

        public abstract long MinPosition { get; }
    }
}
