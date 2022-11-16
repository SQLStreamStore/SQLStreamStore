namespace SqlStreamStore.TestUtils.Postgres
{
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using Npgsql.Logging;

    public abstract class PostgresDatabaseManager
    {
        protected readonly string DatabaseName;

        private bool _started;

        protected string DefaultConnectionString => new NpgsqlConnectionStringBuilder(ConnectionString)
        {
            Database = null
        }.ConnectionString;

        public abstract string ConnectionString { get; }

        static PostgresDatabaseManager()
        {
#if DEBUG
            NpgsqlLogManager.IsParameterLoggingEnabled = true;
            NpgsqlLogManager.Provider = new LibLogNpgsqlLogProvider();
#endif
        }

        protected PostgresDatabaseManager(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public virtual async Task CreateDatabase(CancellationToken cancellationToken = default)
        {
            using(var connection = new NpgsqlConnection(DefaultConnectionString))
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

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

            using(var command = new NpgsqlCommand(commandText, connection))
            {
                return await command.ExecuteScalarAsync(cancellationToken)
                       != null;
            }
        }

        private async Task CreateDatabase(NpgsqlConnection connection, CancellationToken cancellationToken)
        {
            var commandText = $"CREATE DATABASE {DatabaseName}";

            using(var command = new NpgsqlCommand(commandText, connection))
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        public void Dispose()
        {
            if(!_started)
            {
                return;
            }

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

                using(var command = new NpgsqlCommand($"DROP DATABASE {DatabaseName}", connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
