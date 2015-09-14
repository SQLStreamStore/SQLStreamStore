namespace Cedar.EventStore.Postgres.Tests
{
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class PostgresEventStoreTests : EventStoreAcceptanceTests
    {
        public PostgresEventStoreTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {}

        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new PostgresEventStoreFixture();
        }
    }
}