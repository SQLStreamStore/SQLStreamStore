namespace SqlStreamStore.TestUtils.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using global::MySql.Data.MySqlClient;
    using SqlStreamStore.Infrastructure;
    using Xunit.Abstractions;

    public class MySqlDockerDatabaseManager
    {
        private const string DockerImage = "mysql";
        private const string DockerTag = "5.6";
        private const string ContainerName = "sql-stream-store-tests-mysql";

        private readonly ITestOutputHelper _testOutputHelper;
        private readonly string _databaseName;
        private readonly int _tcpPort;
        private readonly DockerContainer _mysqlContainer;
        private string DefaultConnectionString => new MySqlConnectionStringBuilder(ConnectionString)
        {
            Database = null
        }.ConnectionString;

        private MySqlConnectionStringBuilder ConnectionStringBuilder => new MySqlConnectionStringBuilder
        {
            Database = _databaseName,
            Port = (uint) _tcpPort,
            UserID = "root",
            Pooling = true,
            MaximumPoolSize = 100
        };

        public MySqlDockerDatabaseManager(
            ITestOutputHelper testOutputHelper,
            string databaseName,
            int tcpPort = 3306)
        {
            _testOutputHelper = testOutputHelper;
            _databaseName = databaseName;
            _tcpPort = tcpPort;
            _mysqlContainer = new DockerContainer(
                DockerImage,
                DockerTag,
                HealthCheck,
                new Dictionary<int, int>
                {
                    [tcpPort] = tcpPort
                })
            {
                ContainerName = ContainerName,
                Env = new[]
                {
                    "MYSQL_ALLOW_EMPTY_PASSWORD=1"
                }
            };
        }

        public string ConnectionString => ConnectionStringBuilder.ConnectionString;

        public async Task CreateDatabase(CancellationToken cancellationToken = default)
        {
            await _mysqlContainer.TryStart(cancellationToken).WithTimeout(60 * 1000 * 3);

            var commandText = $"CREATE DATABASE IF NOT EXISTS `{_databaseName}`";
            using (var connection = new MySqlConnection(DefaultConnectionString))
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new MySqlCommand(commandText, connection))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }

        private async Task<bool> HealthCheck(CancellationToken cancellationToken)
        {
            try
            {
                using(var connection = new MySqlConnection(DefaultConnectionString))
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                }

                return true;
            }
            catch(Exception ex)
            {
                _testOutputHelper.WriteLine(ex.Message);
            }

            return false;
        }
    }
}