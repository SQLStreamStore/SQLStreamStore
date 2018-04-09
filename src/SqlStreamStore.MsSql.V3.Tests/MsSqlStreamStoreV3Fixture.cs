namespace SqlStreamStore
{
    using System;
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
        private readonly ISqlServerDatabase _localInstance;

        public MsSqlStreamStoreV3Fixture(string schema, bool disableDeletionTracking = false, string databaseNameOverride = null)
        {
            _schema = schema;
            _disableDeletionTracking = disableDeletionTracking;
            _databaseNameOverride = databaseNameOverride;
            _databaseName = databaseNameOverride ?? $"StreamStoreTests-{Guid.NewGuid():n}";
            _localInstance = Environment.OSVersion.IsWindows()
                ? (ISqlServerDatabase) new LocalSqlServerDatabase(_databaseName)
                : new DockerSqlServerDatabase(_databaseName);

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

        public override void Dispose()
        {
            SqlConnection.ClearAllPools();

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
            if(_databaseNameOverride == null)
            {
                await _localInstance.CreateDatabase();
            }
        }

        private string CreateConnectionString()
        {
            var connectionStringBuilder = _localInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }

        private interface ISqlServerDatabase
        {
            SqlConnection CreateConnection();
            SqlConnectionStringBuilder CreateConnectionStringBuilder();
            Task CreateDatabase(CancellationToken cancellationToken = default);
        }

        private class LocalSqlServerDatabase : ISqlServerDatabase
        {
            private readonly string _databaseName;
            private const string ConnectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=master;Integrated Security=SSPI;";

            public LocalSqlServerDatabase(string databaseName)
            {
                _databaseName = databaseName;
            }
            public SqlConnection CreateConnection()
            {
                return new SqlConnection(ConnectionString);
            }

            public SqlConnectionStringBuilder CreateConnectionStringBuilder()
            {
                return new SqlConnectionStringBuilder(ConnectionString);
            }

            public async Task CreateDatabase(CancellationToken cancellationToken = default)
            {
                using(var connection = CreateConnection())
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                    var tempPath = Environment.GetEnvironmentVariable("Temp");
                    var createDatabase = $"CREATE DATABASE [{_databaseName}] on (name='{_databaseName}', "
                                         + $"filename='{tempPath}\\{_databaseName}.mdf')";
                    using (var command = new SqlCommand(createDatabase, connection))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }
                }
            }
        }

        private class DockerSqlServerDatabase : ISqlServerDatabase
        {
            private readonly string _databaseName;
            private readonly DockerContainer _sqlServerContainer;
            private readonly string _password;
            private const string Image = "microsoft/mssql-server-linux";
            private const string Tag = "2017-CU5";
            private const int Port = 1433;

            public DockerSqlServerDatabase(string databaseName)
            {
                _databaseName = databaseName;
                _password = "!01u0Yx19PW";

                _sqlServerContainer = new DockerContainer(
                    Image,
                    Tag,
                    HealthCheck,
                    Port)
                {
                    ContainerName = "sql-stream-store-tests-mssql",
                    Env = new[] { "ACCEPT_EULA=Y", $"SA_PASSWORD={_password}" }
                };
            }

            public SqlConnection CreateConnection()
                => new SqlConnection(CreateConnectionStringBuilder().ConnectionString);

            public SqlConnectionStringBuilder CreateConnectionStringBuilder()
                => new SqlConnectionStringBuilder($"server=localhost,{Port};User Id=sa;Password={_password};Initial Catalog=master");

            public async Task CreateDatabase(CancellationToken cancellationToken = default(CancellationToken))
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
                catch (Exception ex){}

                return false;
            }
        }
    }
}