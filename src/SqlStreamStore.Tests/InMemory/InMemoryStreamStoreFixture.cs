namespace SqlStreamStore.InMemory
{
    using System.Threading.Tasks;
    using SqlStreamStore;

    public class InMemoryStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public override Task<IEventStore> GetEventStore()
        {
            IEventStore eventStore = new InMemoryEventStore(() => GetUtcNow());
            return Task.FromResult(eventStore);
        }
    }
}