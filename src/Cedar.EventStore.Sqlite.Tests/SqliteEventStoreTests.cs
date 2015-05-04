namespace Cedar.EventStore
{
    using Xunit;

    public class SqliteEventStoreTests : EventStoreAcceptanceTests
    {
        [Fact]
        public void SoNCrunchRuns()
        {}

        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new SqliteEventStoreFixture();
        }
    }
}
