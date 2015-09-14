namespace Cedar.EventStore.Postgres.Tests
{
    using System.Threading.Tasks;

    public class PostgresEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        private readonly string _schema;
        public PostgresEventStoreFixture(string schema = "public")
        {
            _schema = schema;
        }

        public override Task<IEventStore> GetEventStore()
        {
            return GetEventStore(_schema);
        }

        private async Task<IEventStore> GetEventStore(string schema)
        {
            var eventStore = new PostgresEventStore(@"Server=127.0.0.1;Port=5432;Database=cedar_tests;User Id=postgres;Password=postgres;", schema);

            await eventStore.DropAll(ignoreErrors: true);
            await eventStore.InitializeStore();

            return eventStore;
        }
    }
}