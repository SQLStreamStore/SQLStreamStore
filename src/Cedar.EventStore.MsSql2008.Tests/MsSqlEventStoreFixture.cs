namespace Cedar.EventStore
{
    using System;
    using System.Data.SqlClient;
    using System.Data.SqlLocalDb;
    using System.Threading.Tasks;

    public class MsSqlEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        private readonly ISqlLocalDbInstance _localDbInstance;
        private readonly string _databaseName;
        private Func<SqlConnection> _createConnectionFunc;

        public MsSqlEventStoreFixture()
        {
            var localDbProvider = new SqlLocalDbProvider
            {
                Version = "11.0"
            };
            _localDbInstance = localDbProvider.GetOrCreateInstance("CedarEventStoreTests");
            _localDbInstance.Start();

            var uniqueName = Guid.NewGuid().ToString().Replace("-", string.Empty);
            _databaseName = $"CedarEventStoreTests_{uniqueName}";
        }

        public override async Task<IEventStore> GetEventStore()
        {
            await CreateDatabase();

            var connectionString = CreateConnectionString();
            _createConnectionFunc = () => new SqlConnection(connectionString);

            var eventStore = new MsSqlEventStore(_createConnectionFunc);

            await eventStore.DropAll(ignoreErrors: true);
            await eventStore.InitializeStore();

            return eventStore;
        }

        public override void Dispose()
        {
            using(var sqlConnection = _createConnectionFunc())
            {
                // Fixes: "Cannot drop database because it is currently in use"
                SqlConnection.ClearPool(sqlConnection);
            }

            using (var connection = _localDbInstance.CreateConnection())
            {
                connection.Open();
                using (var command = new SqlCommand($"DROP DATABASE {_databaseName}", connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        private async Task CreateDatabase()
        {
            using(var connection = _localDbInstance.CreateConnection())
            {
                await connection.OpenAsync();
                using(var command = new SqlCommand($"CREATE DATABASE {_databaseName}", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private string CreateConnectionString()
        {
            var connectionStringBuilder = _localDbInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.IntegratedSecurity = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }
    }
}