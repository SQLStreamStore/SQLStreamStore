namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Npgsql;

    public class PostgresStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public string ConnectionString => _databaseManager.ConnectionString;
        private readonly string _schema;
        private readonly string _databaseName;
        private readonly DatabaseManager _databaseManager;
        
        public PostgresStreamStoreFixture(string schema)
        {
            _schema = schema;
            
            _databaseName = Guid.NewGuid().ToString("n");
            
            _databaseManager = new DatabaseManager(_databaseName);
        }

        public override long MinPosition => 1;

        public override async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };
            var store = new PostgresStreamStore(settings);
            await store.DropAll(ignoreErrors: true);

            return store;
        }

        public async Task<IStreamStore> GetStreamStore(string schema)
        {
            await CreateDatabase();
            
            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow()
            };
            IStreamStore store = new PostgresStreamStore(settings);

            //await store.CreateSchema();

            return store;
        }

        public async Task<PostgresStreamStore> GetPostgresStreamStore()
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };

            var store = new PostgresStreamStore(settings);
            await store.DropAll(ignoreErrors: true);

            return store;
        }

        public override void Dispose()
        {
            _databaseManager.Dispose();
        }

        private Task CreateDatabase() => _databaseManager.CreateDatabase();

        private class DatabaseManager : IDisposable
        {
            private static readonly string s_Tag = Environment.OSVersion.IsWindows() ? WindowsDockerTag : UnixDockerTag;
            private static readonly string s_Image = Environment.OSVersion.IsWindows() ? WindowsImage : UnixImage;
            private const string WindowsImage = "dragnet/postgres-windows";
            private const string WindowsDockerTag = "latest";
            private const string UnixImage = "postgres";
            private const string UnixDockerTag = "9.6.1-alpine";

            private readonly int _tcpPort;
            private readonly string _databaseName;
            private readonly DockerContainer _postgresContainer;
            private readonly TaskCompletionSource<object> _databaseCreated;

            public string ConnectionString => ConnectionStringBuilder.ConnectionString;

            private NpgsqlConnectionStringBuilder ConnectionStringBuilder => new NpgsqlConnectionStringBuilder
            {
                Database = _databaseName,
                Password = Environment.OSVersion.IsWindows()
                    ? "password"
                    : null,
                Port = _tcpPort,
                Username = "postgres"
            };

            public DatabaseManager(string databaseName = null, int tcpPort = 5432)
            {
                _databaseName = databaseName ?? Guid.NewGuid().ToString("n");
                _tcpPort = tcpPort;
                _postgresContainer = new DockerContainer(
                    s_Image,
                    s_Tag,
                    ports: tcpPort);
                _databaseCreated = new TaskCompletionSource<object>();
            }

            public async Task CreateDatabase()
            {
                try
                {
                    await _postgresContainer.TryStart().WithTimeout(60 * 1000);

                    await TryCreateDatabase().WithTimeout(60 * 1000);

                    _databaseCreated.SetResult(null);
                }
                catch(Exception ex)
                {
                    _databaseCreated.SetException(ex);
                }
            }

            private async Task TryCreateDatabase()
            {
                using(var connection = new NpgsqlConnection(ConnectionString))
                {
                    await connection.OpenAsync();

                    using(var command = new NpgsqlCommand($"CREATE DATABASE {_databaseName}", connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
            }

            public void Dispose()
            {
                using(var connection = new NpgsqlConnection(ConnectionString))
                {
                    connection.Open();

                    using(var command = new NpgsqlCommand($"DROP DATABASE {_databaseName}", connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}