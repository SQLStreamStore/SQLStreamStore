namespace Cedar.EventStore
{
    using Cedar.EventStore.Postgres.Tests;

    public partial class EventStoreAcceptanceTests
    {
        private EventStoreAcceptanceTestFixture GetFixture()
        {
            return new PostgresEventStoreFixture();
        }
    }
}