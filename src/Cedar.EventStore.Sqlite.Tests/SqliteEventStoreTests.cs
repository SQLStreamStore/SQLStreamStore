namespace Cedar.EventStore
{
    public class SqliteEventStoreTests : EventStoreAcceptanceTests
    {

        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new SqliteEventStoreFixture();
        }
    }
}
