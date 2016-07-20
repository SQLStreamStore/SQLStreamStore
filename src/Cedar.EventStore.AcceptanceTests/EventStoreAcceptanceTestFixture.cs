namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;

    public abstract class EventStoreAcceptanceTestFixture : IDisposable
    {
        public abstract Task<IEventStore> GetEventStore();

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public virtual void Dispose()
        {}
    }
}