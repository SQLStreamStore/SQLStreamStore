namespace Cedar.EventStore
{
    using Cedar.EventStore.InMemory;

    public partial class EventStoreAcceptanceTests
    {
        public EventStoreAcceptanceTestFixture GetFixture()
        {
            return new InMemoryEventStoreFixture();
        }
    }
}