namespace Cedar.EventStore.Postgres.Tests
{
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class SecondarySchemaTests : EventStoreAcceptanceTests
    {
        public SecondarySchemaTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {}

        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new PostgresEventStoreFixture("secondary_schema");
        }
        
        [Fact]
        public async Task can_store_events_in_different_schemas()
        {
            using (var defaultStore = await GetFixture().GetEventStore())
            using (var secondaryStore = await new PostgresEventStoreFixture("saga_events").GetEventStore())
            {
                const string streamId = "stream-1";
                await defaultStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                await secondaryStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
            }
        }
    }
}