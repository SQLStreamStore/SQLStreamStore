namespace Cedar.EventStore.Postgres.Tests
{
    public class PostgresEventStoreTests : EventStoreAcceptanceTests
    {
        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new PostgresEventStoreFixture();
        }
    }
}