namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;

    public abstract class EventStoreAcceptanceTestFixture : IDisposable
    {
        public abstract Task<IEventStoreClient> GetEventStore();

        public virtual void Dispose()
        { }
    }
}