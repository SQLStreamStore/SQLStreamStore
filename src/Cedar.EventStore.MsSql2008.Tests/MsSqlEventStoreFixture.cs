namespace Cedar.EventStore.MsSql.Tests
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;

    public class MsSqlEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        public override async Task<IEventStore> GetEventStore()
        {
            Func<SqlConnection> createConnectionFunc = () => new SqlConnection(
                @"Data Source=(LocalDB)\v11.0; Integrated Security=True; MultipleActiveResultSets=True");

            var eventStore = new MsSqlEventStore(createConnectionFunc);

            await eventStore.DropAll(ignoreErrors: true);
            await eventStore.InitializeStore();

            return eventStore;
        }
    }
}