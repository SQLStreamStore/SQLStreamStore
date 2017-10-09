namespace SqlStreamStore
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public class MsSqlStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly string _databaseName;
        private readonly ILocalInstance _localInstance;

        public MsSqlStreamStoreFixture(string schema)
        {
            _schema = schema;
            _localInstance = Environment.OSVersion.Platform == PlatformID.Unix ? (ILocalInstance)new DockerInstance() : new LocalInstance();

            var uniqueName = Guid.NewGuid().ToString().Replace("-", string.Empty);
            _databaseName = $"StreamStoreTests-{uniqueName}";

            ConnectionString = CreateConnectionString();
        }

        public override long MinPosition => 0;

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

        public async Task<MsSqlStreamStore> GetUninitializedStreamStore()
        {
            await CreateDatabase();
            
            return new MsSqlStreamStore(new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            });
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
                using (var command = new SqlCommand($"ALTER DATABASE [{_databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", connection))
                {
                    command.ExecuteNonQuery();
                }
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
                await connection.OpenAsync().NotOnCapturedContext();
                var tempPath = System.IO.Path.GetTempPath();
                var createDatabase = $"CREATE DATABASE [{_databaseName}] on (name='{_databaseName}', "
                                     + $"filename='{tempPath}\\{_databaseName}.mdf')";
                using (var command = new SqlCommand(createDatabase, connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private string CreateConnectionString()
        {
            var connectionStringBuilder = _localInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }

        private interface ILocalInstance
        {
            SqlConnection CreateConnection();
            SqlConnectionStringBuilder CreateConnectionStringBuilder();
        }

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

        private class DockerInstance : ILocalInstance
        {
            private readonly string connectionString = @"Data Source=127.0.0.1,1433;Initial Catalog=master;User ID=sa;Password=SqlStreamSt0re;";

            public SqlConnection CreateConnection()
            {
                return new SqlConnection(connectionString);
            }

            public SqlConnectionStringBuilder CreateConnectionStringBuilder()
            {
                return new SqlConnectionStringBuilder(connectionString);
            }
        }
    }
}