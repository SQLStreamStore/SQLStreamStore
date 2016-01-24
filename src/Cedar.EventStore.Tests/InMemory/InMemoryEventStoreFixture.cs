namespace Cedar.EventStore.InMemory
{
    using System.Threading.Tasks;

    public class InMemoryEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        public override Task<IEventStore> GetEventStore()
        {
            IEventStore eventStore = new InMemoryEventStore();
            return Task.FromResult(eventStore);
        }
    }
}