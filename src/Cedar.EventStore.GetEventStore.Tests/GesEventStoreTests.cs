namespace Cedar.EventStore
{
    using Xunit.Abstractions;

    public class GesEventStoreTests : EventStoreAcceptanceTests
    {
        public GesEventStoreTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {}

        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new GesEventStoreFixture();
        }
    }
}