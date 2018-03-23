namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Npgsql;
    using Npgsql.Logging;
    using Xunit.Abstractions;

    public class PostgresStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        private string ConnectionString => _databaseManager.ConnectionString;
        private readonly string _schema;
        private readonly Guid _databaseId;
        private readonly DatabaseManager _databaseManager;
        
        public PostgresStreamStoreFixture(string schema, ITestOutputHelper testOutputHelper)
        {
            _schema = schema;

            _databaseId = Guid.NewGuid();
            
            _databaseManager = new DatabaseManager(testOutputHelper, _databaseId);
        }

        public override long MinPosition => 0;

        public override async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };

            var store = new PostgresStreamStore(settings);

            await store.CreateSchema();

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
            var store = new PostgresStreamStore(settings);

            await store.CreateSchema();

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

            return new PostgresStreamStore(settings);
        }

        public override void Dispose()
        {
            _databaseManager.Dispose();
        }

        private Task CreateDatabase() => _databaseManager.CreateDatabase();

        private class DatabaseManager : IDisposable
        {
            private static readonly string s_tag = Environment.OSVersion.IsWindows() ? WindowsDockerTag : UnixDockerTag;
            private static readonly string s_image = Environment.OSVersion.IsWindows() ? WindowsImage : UnixImage;
            private const string WindowsImage = "postgres";
            private const string WindowsDockerTag = "9.6.1-alpine";
            private const string UnixImage = "postgres";
            private const string UnixDockerTag = "9.6.1-alpine";

            private readonly ITestOutputHelper _output;
            private readonly int _tcpPort;
            private readonly string _databaseName;
            private readonly DockerContainer _postgresContainer;
            private bool _started;

            public string ConnectionString => ConnectionStringBuilder.ConnectionString;

            private NpgsqlConnectionStringBuilder ConnectionStringBuilder => new NpgsqlConnectionStringBuilder
            {
                Database = _databaseName,
                Password = Environment.OSVersion.IsWindows()
                    ? "password"
                    : null,
                Port = _tcpPort,
                Username = "postgres",
                Host = "localhost"
            };
            
            private string DefaultConnectionString => new NpgsqlConnectionStringBuilder(ConnectionString)
            {
                Database = null
            }.ConnectionString;

            static DatabaseManager()
            {
                NpgsqlLogManager.IsParameterLoggingEnabled = true;
                NpgsqlLogManager.Provider = new XunitNpgsqlLogProvider();
            }

            public DatabaseManager(ITestOutputHelper output, Guid databaseId, int tcpPort = 5432)
            {
                XunitNpgsqlLogProvider.s_CurrentOutput = _output = output;
                _databaseName = $"test_{databaseId:n}";
                _tcpPort = tcpPort;
                _postgresContainer = new DockerContainer(
                    s_image,
                    s_tag,
                    ports: tcpPort)
                {
                    ContainerName = "sql-stream-store-tests-postgres"
                };
            }

            public async Task CreateDatabase()
            {
                await _postgresContainer.TryStart().WithTimeout(60 * 1000);

                using(var connection = new NpgsqlConnection(DefaultConnectionString))
                {
                    await connection.OpenAsync();

                    if(!await DatabaseExists(connection))
                    {
                        await CreateDatabase(connection);
                    }
                }

                _started = true;
            }

            private async Task<bool> DatabaseExists(NpgsqlConnection connection)
            {
                var commandText = $"SELECT 1 FROM pg_database WHERE datname = '{_databaseName}'";

                try
                {
                    using(var command = new NpgsqlCommand(commandText, connection))
                    {
                        return await command.ExecuteScalarAsync() != null;
                    }
                }
                catch(Exception ex)
                {
                    _output.WriteLine($@"Attempted to execute ""{commandText}"" but failed: {ex}");
                    throw;
                }
            }

            private async Task CreateDatabase(NpgsqlConnection connection)
            {
                var commandText = $"CREATE DATABASE {_databaseName}";

                try
                {
                    using(var command = new NpgsqlCommand(commandText, connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
                catch(Exception ex)
                {
                    _output.WriteLine($@"Attempted to execute ""{commandText}"" but failed: {ex}");
                    throw;
                }
            }

            public void Dispose()
            {
                if(!_started)
                {
                    return;
                }

                var commandText = $"DROP DATABASE {_databaseName}";

                try
                {
                    using(var connection = new NpgsqlConnection(DefaultConnectionString))
                    {
                        connection.Open();

                        using(var command = new NpgsqlCommand($"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity  WHERE pg_stat_activity.datname = '{_databaseName}' AND pid <> pg_backend_pid()", connection))
                        {
                            command.ExecuteNonQuery();
                        }

                        using(var command = new NpgsqlCommand(commandText, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                }
                catch(Exception ex)
                {
                    _output.WriteLine($@"Attempted to execute ""{commandText}"" but failed: {ex}");
                }
            }
        }
        
        private class XunitNpgsqlLogger : NpgsqlLogger
        {
            private readonly ITestOutputHelper _output;
            private readonly string _name;

            public XunitNpgsqlLogger(ITestOutputHelper output, string name)
            {
                _output = output;
                _name = name;
            }

            public override bool IsEnabled(NpgsqlLogLevel level) => true;

            public override void Log(NpgsqlLogLevel level, int connectorId, string msg, Exception exception = null)
            {
                _output.WriteLine($@"[{level:G}] [{_name}] (Connector Id: {connectorId}); {msg}; (Exception: {exception?.ToString() ?? "<none>"})");
            }
        }

        private class XunitNpgsqlLogProvider : INpgsqlLoggingProvider
        {
            internal static ITestOutputHelper s_CurrentOutput;
            
            public NpgsqlLogger CreateLogger(string name) => new XunitNpgsqlLogger(s_CurrentOutput, name);
        }
    }
}