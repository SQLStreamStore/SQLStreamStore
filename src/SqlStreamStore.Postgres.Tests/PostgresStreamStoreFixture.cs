namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using Npgsql.Logging;
    using SqlStreamStore.Infrastructure;
    using Xunit.Abstractions;

    public class PostgresStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public string ConnectionString => _databaseManager.ConnectionString;
        private readonly string _schema;
        private readonly IDatabaseManager _databaseManager;

        public PostgresStreamStoreFixture(string schema)
            : this(schema, new ConsoleTestoutputHelper())
        { }

        public PostgresStreamStoreFixture(string schema, ITestOutputHelper testOutputHelper)
        {
            _schema = schema;

            _databaseManager = new DockerDatabaseManager(testOutputHelper, $"test_{Guid.NewGuid():n}");
        }

        public PostgresStreamStoreFixture(string schema, string connectionString)
        {
            _schema = schema;

            _databaseManager = new ServerDatabaseManager(
                new ConsoleTestoutputHelper(),
                $"test_{Guid.NewGuid():n}",
                connectionString);
        }

        public override long MinPosition => 0;

        public override int MaxSubscriptionCount => 90;

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
            var store = await GetUninitializedPostgresStreamStore();

            await store.CreateSchema();

            return store;
        }

        public async Task<PostgresStreamStore> GetUninitializedPostgresStreamStore()
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
            _databaseManager?.Dispose();
        }

        private Task CreateDatabase() => _databaseManager.CreateDatabase();

        private interface IDatabaseManager : IDisposable
        {
            string ConnectionString { get; }
            Task CreateDatabase(CancellationToken cancellationToken = default(CancellationToken));
        }

        private abstract class DatabaseManager : IDatabaseManager
        {
            protected readonly string DatabaseName;
            protected readonly ITestOutputHelper Output;

            private bool _started;

            protected string DefaultConnectionString => new NpgsqlConnectionStringBuilder(ConnectionString)
            {
                Database = null
            }.ConnectionString;

            public abstract string ConnectionString { get; }

            static DatabaseManager()
            {
#if DEBUG
                NpgsqlLogManager.IsParameterLoggingEnabled = true;
                NpgsqlLogManager.Provider = new XunitNpgsqlLogProvider();
#endif
            }

            protected DatabaseManager(ITestOutputHelper output, string databaseName)
            {
                XunitNpgsqlLogProvider.s_CurrentOutput = Output = output;
                DatabaseName = databaseName;
            }

            public virtual async Task CreateDatabase(CancellationToken cancellationToken = default(CancellationToken))
            {
                using(var connection = new NpgsqlConnection(DefaultConnectionString))
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                    if(!await DatabaseExists(connection, cancellationToken))
                    {
                        await CreateDatabase(connection, cancellationToken);
                    }
                }

                _started = true;
            }

            private async Task<bool> DatabaseExists(NpgsqlConnection connection, CancellationToken cancellationToken)
            {
                var commandText = $"SELECT 1 FROM pg_database WHERE datname = '{DatabaseName}'";

                try
                {
                    using(var command = new NpgsqlCommand(commandText, connection))
                    {
                        return await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext()
                               != null;
                    }
                }
                catch(Exception ex)
                {
                    Output.WriteLine($@"Attempted to execute ""{commandText}"" but failed: {ex}");
                    throw;
                }
            }

            private async Task CreateDatabase(NpgsqlConnection connection, CancellationToken cancellationToken)
            {
                var commandText = $"CREATE DATABASE {DatabaseName}";

                try
                {
                    using(var command = new NpgsqlCommand(commandText, connection))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }
                }
                catch(Exception ex)
                {
                    Output.WriteLine($@"Attempted to execute ""{commandText}"" but failed: {ex}");
                    throw;
                }
            }

            public void Dispose()
            {
                if(!_started)
                {
                    return;
                }

                var commandText = $"DROP DATABASE {DatabaseName}";

                try
                {
                    using(var connection = new NpgsqlConnection(DefaultConnectionString))
                    {
                        connection.Open();

                        using(var command =
                            new NpgsqlCommand(
                                $"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity  WHERE pg_stat_activity.datname = '{DatabaseName}' AND pid <> pg_backend_pid()",
                                connection))
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
                    Output.WriteLine($@"Attempted to execute ""{commandText}"" but failed: {ex}");
                }
            }
        }

        private class DockerDatabaseManager : DatabaseManager
        {
            private const string DockerImage = "postgres";
            private const string DockerTag = "9.6.6-alpine";
            private const string ContainerName = "sql-stream-store-tests-postgres";

            private readonly int _tcpPort;
            private readonly DockerContainer _postgresContainer;

            public override string ConnectionString => ConnectionStringBuilder.ConnectionString;

            private NpgsqlConnectionStringBuilder ConnectionStringBuilder => new NpgsqlConnectionStringBuilder
            {
                Database = DatabaseName,
                Password = Environment.OSVersion.IsWindows()
                    ? "password"
                    : null,
                Port = _tcpPort,
                Username = "postgres",
                Host = "localhost",
                Pooling = true,
                MaxPoolSize = 1024
            };

            public DockerDatabaseManager(ITestOutputHelper output, string databaseName, int tcpPort = 5432)
                : base(output, databaseName)
            {
                _tcpPort = tcpPort;
                _postgresContainer = new DockerContainer(
                    DockerImage,
                    DockerTag,
                    HealthCheck,
                    ports: tcpPort)
                {
                    ContainerName = ContainerName,
                    //   Env = new[] { @"PGOPTIONS=-N 1024" }
                };
            }

            public override async Task CreateDatabase(CancellationToken cancellationToken = default(CancellationToken))
            {
                await _postgresContainer.TryStart(cancellationToken).WithTimeout(60 * 1000 * 3);

                await base.CreateDatabase(cancellationToken);
            }

            private async Task<bool> HealthCheck(CancellationToken cancellationToken)
            {
                try
                {
                    using(var connection = new NpgsqlConnection(DefaultConnectionString))
                    {
                        await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                    }

                    return true;
                }
                catch(Exception ex)
                {
                    Output.WriteLine(ex.Message);
                }

                return false;
            }
        }

        private class ServerDatabaseManager : DatabaseManager
        {
            public override string ConnectionString { get; }

            public ServerDatabaseManager(ITestOutputHelper output, string databaseName, string connectionString)
                : base(output, databaseName)
            {
                ConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
                {
                    Database = DatabaseName
                }.ConnectionString;
            }
        }

        private class ConsoleTestoutputHelper : ITestOutputHelper
        {
            public void WriteLine(string message) => Console.Write(message);
            public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);
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
                => _output.WriteLine(
                    $@"[{level:G}] [{_name}] (Connector Id: {connectorId}); {msg}; {
                            FormatOptionalException(exception)
                        }");

            private static string FormatOptionalException(Exception exception)
                => exception == null ? string.Empty : $"(Exception: {exception})";
        }

        private class XunitNpgsqlLogProvider : INpgsqlLoggingProvider
        {
            internal static ITestOutputHelper s_CurrentOutput;

            public NpgsqlLogger CreateLogger(string name) => new XunitNpgsqlLogger(s_CurrentOutput, name);
        }
    }
}