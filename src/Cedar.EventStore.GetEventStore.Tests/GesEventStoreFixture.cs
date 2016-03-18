namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;

    public class GesEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        public override Task<IEventStore> GetEventStore()
        {
            throw new NotImplementedException();
        }
    }
}