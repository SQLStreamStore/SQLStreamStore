namespace StreamStore
{
    using System;
    using System.Threading.Tasks;
    using StreamStore.Infrastructure;

    public abstract class StreamStoreAcceptanceTestFixture : IDisposable
    {
        public abstract Task<IEventStore> GetEventStore();

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public virtual void Dispose()
        {}
    }
}