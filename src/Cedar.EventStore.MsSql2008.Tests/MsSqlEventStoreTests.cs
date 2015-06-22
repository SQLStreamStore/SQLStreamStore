namespace Cedar.EventStore.MsSql.Tests
{
    using Xunit.Abstractions;

    public class MsSqlEventStoreTests : EventStoreAcceptanceTests
    {
        public MsSqlEventStoreTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {}

        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new MsSqlEventStoreFixture();
        }
    }
}