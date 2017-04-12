namespace SqlStreamStore
{
    using System;
    using System.Data.SqlClient;
#if NET46
    using System.Data.SqlLocalDb;
#endif
    using System.Linq;
    using System.Threading.Tasks;

    public class MsSqlStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly string _databaseName;
        private readonly ILocalInstance _localInstance;

        public MsSqlStreamStoreFixture(string schema)
        {
            _schema = schema;
            _localInstance = new LocalInstance();

            var uniqueName = Guid.NewGuid().ToString().Replace("-", string.Empty);
            _databaseName = $"StreamStoreTests-{uniqueName}";

            ConnectionString = CreateConnectionString();
        }

        public override async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            return await GetStreamStore(_schema);
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

        public async Task<MsSqlStreamStore> GetStreamStore_v1Schema()
        {
            await CreateDatabase();
            var settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };
            var store = new MsSqlStreamStore(settings);
            await store.CreateSchema_v1_ForTests();

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

            using (var connection = _localInstance.CreateConnection())
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
            using(var connection = _localInstance.CreateConnection())
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
            var connectionStringBuilder = _localInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.IntegratedSecurity = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }

        private interface ILocalInstance
        {
            SqlConnection CreateConnection();
            SqlConnectionStringBuilder CreateConnectionStringBuilder();
        }

#if NETCOREAPP1_1
        private class LocalInstance : ILocalInstance
        {
            private readonly string connectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=master;Integrated Security=SSPI;";

            public SqlConnection CreateConnection()
            {
                return new SqlConnection(connectionString);
            }

            public SqlConnectionStringBuilder CreateConnectionStringBuilder()
            {
                return new SqlConnectionStringBuilder(connectionString);
            }
        }
#elif NET46
        private class LocalInstance : ILocalInstance
        {
            private readonly ISqlLocalDbInstance _localDbInstance;

            private static readonly string s_sqlLocalDbProviderVersionToUse = new SqlLocalDbProvider()
                    .GetVersions()
                    .Where(provider => provider.Exists)
                    .Max(provider => provider.Version)
                    .ToString(2);

            public LocalInstance()
            {
                var localDbProvider = new SqlLocalDbProvider
                {
                    Version = s_sqlLocalDbProviderVersionToUse
                };
                _localDbInstance = localDbProvider.GetOrCreateInstance("StreamStoreTests");
                _localDbInstance.Start();
            }

            public SqlConnection CreateConnection()
            {
                return _localDbInstance.CreateConnection();
            }

            public SqlConnectionStringBuilder CreateConnectionStringBuilder()
            {
                return _localDbInstance.CreateConnectionStringBuilder();
            }
        }
#endif
    }
}