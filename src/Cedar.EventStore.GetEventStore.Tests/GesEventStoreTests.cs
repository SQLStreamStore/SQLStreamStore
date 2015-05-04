namespace Cedar.EventStore
{
    public class GesEventStoreTests : EventStoreAcceptanceTests
    {
        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new GesEventStoreFixture();
        }
    }
}