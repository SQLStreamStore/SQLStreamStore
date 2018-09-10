namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public class MsSqlStreamStoreV3Fixture : StreamStoreAcceptanceTestFixture
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly bool _disableDeletionTracking;
        private readonly string _databaseNameOverride;
        private readonly string _databaseName;
        private readonly DockerSqlServerDatabase _databaseInstance;

        public MsSqlStreamStoreV3Fixture(string schema, bool disableDeletionTracking = false, string databaseNameOverride = null)
        {
            _schema = schema;
            _disableDeletionTracking = disableDeletionTracking;
            _databaseNameOverride = databaseNameOverride;
            _databaseName = databaseNameOverride ?? $"StreamStoreTests-{Guid.NewGuid():n}";
            _databaseInstance = new DockerSqlServerDatabase(_databaseName);

            ConnectionString = CreateConnectionString();
        }

        public override long MinPosition => 0;

        public override int MaxSubscriptionCount => 500;

        public override async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            return await GetStreamStore(_schema);
        }

        public async Task<IStreamStore> GetStreamStore(string schema)
        {
            var settings = new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow(),
                DisableDeletionTracking = _disableDeletionTracking
            };
            var store = new MsSqlStreamStoreV3(settings);
            await store.CreateSchema();

            return store;
        }

        public async Task<MsSqlStreamStoreV3> GetUninitializedStreamStore()
        {
            await CreateDatabase();
            
            return new MsSqlStreamStoreV3(new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            });
        }

        public async Task<MsSqlStreamStoreV3> GetMsSqlStreamStore()
        {
            await CreateDatabase();

            var settings = new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };

            var store = new MsSqlStreamStoreV3(settings);
            await store.CreateSchema();

            return store;
        }

        public async Task<MsSqlStreamStore> GetMsSqlStreamStoreV2()
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
            SqlConnection.ClearAllPools();

            using (var connection = _databaseInstance.CreateConnection())
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
            if(_databaseNameOverride == null)
            {
                await _databaseInstance.CreateDatabase();
            }
        }

        private string CreateConnectionString()
        {
            var connectionStringBuilder = _databaseInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }

        private class DockerSqlServerDatabase
        {
            private readonly string _databaseName;
            private readonly DockerContainer _sqlServerContainer;
            private readonly string _password;
            private const string Image = "microsoft/mssql-server-linux";
            private const string Tag = "2017-CU9";
            private const int Port = 11433;

            public DockerSqlServerDatabase(string databaseName)
            {
                _databaseName = databaseName;
                _password = "!01u0Yx19PW";

                var ports = new Dictionary<int, int>
                {
                    { 1433, Port }
                };

                _sqlServerContainer = new DockerContainer(
                    Image,
                    Tag,
                    HealthCheck,
                    ports)
                {
                    ContainerName = "sql-stream-store-tests-mssql-v3",
                    Env = new[] { "ACCEPT_EULA=Y", $"SA_PASSWORD={_password}" }
                };
            }

            public SqlConnection CreateConnection()
                => new SqlConnection(CreateConnectionStringBuilder().ConnectionString);

            public SqlConnectionStringBuilder CreateConnectionStringBuilder()
                => new SqlConnectionStringBuilder($"server=localhost,{Port};User Id=sa;Password={_password};Initial Catalog=master");

            public async Task CreateDatabase(CancellationToken cancellationToken = default)
            {
                await _sqlServerContainer.TryStart(cancellationToken).WithTimeout(3 * 60 * 1000);

                using(var connection = CreateConnection())
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                    using (var command = new SqlCommand($"CREATE DATABASE [{_databaseName}]", connection))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }
                }
            }

            private async Task<bool> HealthCheck(CancellationToken cancellationToken)
            {
                try
                {
                    using(var connection = CreateConnection())
                    {
                        await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                        return true;
                    }
                }
                catch (Exception) { }

                return false;
            }
        }
    }
}