namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;

    public abstract class EventStoreAcceptanceTestFixture : IDisposable
    {
        public abstract Task<IEventStore> GetEventStore();

        public virtual GetUtcNow GetUtcNow
        {
            get { return SystemClock.GetUtcNow; }
        }

        public virtual void Dispose()
        { }
    }
}