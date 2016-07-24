namespace SqlStreamStore
{
    using System;
    using System.Data.SqlClient;
    using System.Data.SqlLocalDb;
    using System.Threading.Tasks;

    public class MsSqlStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly ISqlLocalDbInstance _localDbInstance;
        private readonly string _databaseName;

        public MsSqlStreamStoreFixture(string schema)
        {
            _schema = schema;
            var localDbProvider = new SqlLocalDbProvider
            {
                Version = "10.5"
            };
            _localDbInstance = localDbProvider.GetOrCreateInstance("StreamStoreTests");
            _localDbInstance.Start();

            var uniqueName = Guid.NewGuid().ToString().Replace("-", string.Empty);
            _databaseName = $"StreamStoreTests_{uniqueName}";

            ConnectionString = CreateConnectionString();
        }

        public override async Task<IEventStore> GetEventStore()
        {
            await CreateDatabase();

            var settings = new MsSqlEventStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };
            var eventStore = new MsSqlEventStore(settings);
            await eventStore.DropAll(ignoreErrors: true);
            await eventStore.InitializeStore();

            return eventStore;
        }

        public async Task<IEventStore> GetEventStore(string schema)
        {
            var settings = new MsSqlEventStoreSettings(ConnectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow()
            };
            var eventStore = new MsSqlEventStore(settings);
            await eventStore.InitializeStore();

            return eventStore;
        }

        public async Task<MsSqlEventStore> GetMsSqlEventStore()
        {
            await CreateDatabase();

            var settings = new MsSqlEventStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };

            var eventStore = new MsSqlEventStore(settings);
            await eventStore.DropAll(ignoreErrors: true);
            await eventStore.InitializeStore();

            return eventStore;
        }

        public override void Dispose()
        {
            using(var sqlConnection = new SqlConnection(ConnectionString))
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