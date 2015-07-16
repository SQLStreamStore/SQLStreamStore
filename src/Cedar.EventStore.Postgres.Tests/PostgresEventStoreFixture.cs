namespace Cedar.EventStore.Postgres.Tests
{
    using System.Threading.Tasks;

    public class PostgresEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        public override async Task<IEventStore> GetEventStore()
        {
            var eventStore = new PostgresEventStore(@"Server=127.0.0.1;Port=5432;Database=cedar_tests;User Id=postgres;Password=postgres;");

            await eventStore.DropAll(ignoreErrors: true);
            await eventStore.InitializeStore();

            return eventStore;
        }
    }
}