namespace Cedar.EventStore.Postgres.Tests
{
    using System;
    using System.Threading.Tasks;

    using Npgsql;

    public class PostgresEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        public override async Task<IEventStore> GetEventStore()
        {
            Func<NpgsqlConnection> createConnectionFunc = () => new NpgsqlConnection(
                @"Server=127.0.0.1;Port=5432;Database=cedar_tests;User Id=postgres;Password=postgres;");

            var eventStore = new PostgresEventStore(createConnectionFunc);

            await eventStore.DropAll(ignoreErrors: true);
            await eventStore.InitializeStore();

            return eventStore;
        }
    }
}