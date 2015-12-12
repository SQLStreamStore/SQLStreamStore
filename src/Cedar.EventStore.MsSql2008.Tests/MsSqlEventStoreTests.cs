namespace Cedar.EventStore
{
    public partial class EventStoreAcceptanceTests
    {
        public EventStoreAcceptanceTestFixture GetFixture()
        {
            return new MsSqlEventStoreFixture();
        }
    }
}