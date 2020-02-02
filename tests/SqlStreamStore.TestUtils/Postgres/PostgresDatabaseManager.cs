namespace SqlStreamStore.TestUtils.Postgres
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using Npgsql.Logging;
    using SqlStreamStore.Infrastructure;
    using Xunit.Abstractions;

    public abstract class PostgresDatabaseManager
    {
        protected readonly string DatabaseName;
        protected readonly ITestOutputHelper TestOutputHelper;

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

        protected PostgresDatabaseManager(ITestOutputHelper testOutputHelper, string databaseName)
        {
            TestOutputHelper = testOutputHelper;
            DatabaseName = databaseName;
        }

        public virtual async Task CreateDatabase(CancellationToken cancellationToken = default)
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
                TestOutputHelper.WriteLine($@"Attempted to execute ""{commandText}"" but failed: {ex}");
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
                TestOutputHelper.WriteLine($@"Attempted to execute ""{commandText}"" but failed: {ex}");
                throw;
            }
        }

        public void Dispose()
        {
            if(!_started)
            {
                return;
            }

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

                    using(var command = new NpgsqlCommand($"DROP DATABASE {DatabaseName}", connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch(Exception ex)
            {
                TestOutputHelper.WriteLine($@"Attempted to execute ""{$"DROP DATABASE {DatabaseName}"}"" but failed: {ex}");
            }
        }
    }
}