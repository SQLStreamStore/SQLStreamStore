namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore;
    using SqlStreamStore.Infrastructure;

    public class MsSqlStreamStoreDb : IDisposable
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly bool _deleteDatabaseOnDispose;
        private readonly string _databaseName;
        private readonly DockerSqlServerDatabase _databaseInstance;

        public MsSqlStreamStoreDb(string schema, bool deleteDatabaseOnDispose = true)
        {
            _schema = schema;
            _deleteDatabaseOnDispose = deleteDatabaseOnDispose;
            _databaseName = $"sss-v2-{Guid.NewGuid():n}";
            _databaseInstance = new DockerSqlServerDatabase(_databaseName);

            ConnectionString = CreateConnectionString();
        }

        public async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            return await GetStreamStore(_schema);
        }

        public async Task<IStreamStore> GetStreamStore(string schema)
        {
            var settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = schema,
            };
            var store = new MsSqlStreamStore(settings);
            await store.CreateSchema();

            return store;
        }

        public void Dispose()
        {
            if (!_deleteDatabaseOnDispose)
            {
                return;
            }
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                // Fixes: "Cannot drop database because it is currently in use"
                SqlConnection.ClearPool(sqlConnection);
            }
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

        private Task CreateDatabase() => _databaseInstance.CreateDatabase();

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
            private const string Password = "!Passw0rd";
            private const string Image = "microsoft/mssql-server-linux";
            private const string Tag = "2017-CU9";
            private const int Port = 11433;

            public DockerSqlServerDatabase(string databaseName)
            {
                _databaseName = databaseName;

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
                    ContainerName = "sql-stream-store-tests-mssql",
                    Env = new[] { "ACCEPT_EULA=Y", $"SA_PASSWORD={Password}" }
                };
            }

            public SqlConnection CreateConnection()
                => new SqlConnection(CreateConnectionStringBuilder().ConnectionString);

            public SqlConnectionStringBuilder CreateConnectionStringBuilder()
                => new SqlConnectionStringBuilder($"server=localhost,{Port};User Id=sa;Password={Password};Initial Catalog=master");

            public async Task CreateDatabase(CancellationToken cancellationToken = default)
            {
                await _sqlServerContainer.TryStart(cancellationToken).WithTimeout(3 * 60 * 1000);

                using (var connection = CreateConnection())
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                    var createCommand = $@"CREATE DATABASE [{_databaseName}]
ALTER DATABASE [{_databaseName}] SET SINGLE_USER
ALTER DATABASE [{_databaseName}] SET COMPATIBILITY_LEVEL=110
ALTER DATABASE [{_databaseName}] SET MULTI_USER";

                    using (var command = new SqlCommand(createCommand, connection))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }
                }
            }

            private async Task<bool> HealthCheck(CancellationToken cancellationToken)
            {
                try
                {
                    using (var connection = CreateConnection())
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