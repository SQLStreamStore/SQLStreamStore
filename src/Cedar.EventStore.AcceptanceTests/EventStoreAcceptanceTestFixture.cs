namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;

    public abstract class EventStoreAcceptanceTestFixture : IDisposable
    {
        public abstract Task<IEventStore> GetEventStore();

        public virtual GetUtcNow GetUtcNow => SystemClock.GetUtcNow;

        public virtual void Dispose()
        { }
    }
}