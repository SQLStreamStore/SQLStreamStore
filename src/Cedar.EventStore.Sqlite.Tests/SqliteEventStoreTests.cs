namespace Cedar.EventStore
{
    public partial class EventStoreAcceptanceTests
    {
        private EventStoreAcceptanceTestFixture GetFixture()
        {
            return new SqliteEventStoreFixture();
        }
    }
}