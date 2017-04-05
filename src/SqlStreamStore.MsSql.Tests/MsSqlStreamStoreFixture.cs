namespace SqlStreamStore
{
    using System;
    using System.Data.SqlClient;
    using System.Data.SqlLocalDb;
    using System.Linq;
    using System.Threading.Tasks;

    public class MsSqlStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly string _databaseName;

        private static readonly ISqlLocalDbInstance s_localDbInstance;

        private static readonly string s_sqlLocalDbProviderVersionToUse = new SqlLocalDbProvider()
                .GetVersions()
                .Where(provider => provider.Exists)
                .Max(provider => provider.Version)
                .ToString(2);

        static MsSqlStreamStoreFixture()
        {
            var localDbProvider = new SqlLocalDbProvider
            {
                Version = s_sqlLocalDbProviderVersionToUse
            };
            s_localDbInstance = new TemporarySqlLocalDbInstance($"SSS-{Guid.NewGuid()}", localDbProvider);
        }

        public MsSqlStreamStoreFixture(string schema)
        {
            _schema = schema;

            var uniqueName = Guid.NewGuid().ToString("N");
            _databaseName = $"StreamStoreTests-{uniqueName}";

            ConnectionString = CreateConnectionString();
        }

        public override async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            var settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };
            var store = new MsSqlStreamStore(settings);
            await store.DropAll(ignoreErrors: true);
            await store.CreateSchema();

            return store;
        }

        public async Task<IStreamStore> GetStreamStore(string schema)
        {
            var settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow()
            };
            var store = new MsSqlStreamStore(settings);
            await store.CreateSchema();

            return store;
        }

        public async Task<MsSqlStreamStore> GetMsSqlStreamStore()
        {
            await CreateDatabase();

            var settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };

            var store = new MsSqlStreamStore(settings);
            await store.DropAll(ignoreErrors: true);
            await store.CreateSchema();

            return store;
        }

        public override void Dispose()
        {
            using(var sqlConnection = new SqlConnection(ConnectionString))
            {
                // Fixes: "Cannot drop database because it is currently in use"
                SqlConnection.ClearPool(sqlConnection);
            }

            using (var connection = s_localDbInstance.CreateConnection())
            {
                connection.Open();
                using (var command = new SqlCommand($"DROP DATABASE [{_databaseName}]", connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        private async Task CreateDatabase()
        {
            using(var connection = s_localDbInstance.CreateConnection())
            {
                await connection.OpenAsync();
                using(var command = new SqlCommand($"CREATE DATABASE  [{_databaseName}]", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private string CreateConnectionString()
        {
            var connectionStringBuilder = s_localDbInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.IntegratedSecurity = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }
    }
}