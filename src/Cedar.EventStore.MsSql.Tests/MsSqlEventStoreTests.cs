namespace Cedar.EventStore.MsSql.Tests
{
    public class MsSqlEventStoreTests : EventStoreAcceptanceTests
    {
        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new MsSqlEventStoreFixture();
        }
    }
}