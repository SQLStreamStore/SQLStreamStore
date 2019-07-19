namespace SqlStreamStore.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using global::MySql.Data.MySqlClient;
    using MySqlConnector.Logging;
    using SqlStreamStore.Infrastructure;
    using Xunit.Abstractions;

    public abstract class MySqlDatabaseManager
    {
        protected readonly string DatabaseName;
        protected readonly ITestOutputHelper TestOutputHelper;

        private bool _started;

        protected string DefaultConnectionString => new MySqlConnectionStringBuilder(ConnectionString)
        {
            Database = null
        }.ConnectionString;

        public abstract string ConnectionString { get; }

        static MySqlDatabaseManager()
        {
#if DEBUG
            MySqlConnectorLogManager.Provider = new LibLogMySqlConnectorLoggerProvider();
#endif
        }

        protected MySqlDatabaseManager(ITestOutputHelper testOutputHelper, string databaseName)
        {
            TestOutputHelper = testOutputHelper;
            DatabaseName = databaseName;
        }

        public virtual async Task CreateDatabase(CancellationToken cancellationToken = default)
        {
            var commandText = $"CREATE DATABASE IF NOT EXISTS `{DatabaseName}`";
            using(var connection = new MySqlConnection(DefaultConnectionString))
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new MySqlCommand(commandText, connection))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                }
            }

            _started = true;
        }

        public void Dispose()
        {
            if(!_started)
            {
                return;
            }

            try
            {
                var processIds = new List<int>();

                using(var connection = new MySqlConnection(DefaultConnectionString))
                {
                    connection.Open();

                    using(var command = new MySqlCommand($"DROP DATABASE {DatabaseName}", connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    using(var command = new MySqlCommand(
                        $"SELECT ID FROM INFORMATION_SCHEMA.PROCESSLIST WHERE db = '{DatabaseName}' ORDER BY TIME DESC",
                        connection)
                    )
                    using(var reader = command.ExecuteReader())
                    {
                        while(reader.Read())
                        {
                            processIds.Add(reader.GetInt32(0));
                        }
                    }

                    foreach(var processId in processIds)
                    {
                        try
                        {
                            using(var command = new MySqlCommand($"KILL {processId}", connection))
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                        catch(MySqlException ex) when(ex.Number == 1094
                        ) // unknown thread id. means there is nothing to do
                        { }
                    }
                }
            }
            catch(MySqlException ex)
            {
                TestOutputHelper.WriteLine(
                    $@"Attempted to execute ""{$"DROP DATABASE {DatabaseName}"}"" but failed. Number: {ex.Number}; SqlState: {ex.SqlState} {ex}");
                throw;
            }
            catch(Exception ex)
            {
                TestOutputHelper.WriteLine(
                    $@"Attempted to execute ""{$"DROP DATABASE {DatabaseName}"}"" but failed. {ex}");
                throw;
            }
        }
    }
}