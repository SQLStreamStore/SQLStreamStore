namespace SqlStreamStore.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using global::MySql.Data.MySqlClient;
    using SqlStreamStore;
    using SqlStreamStore.Infrastructure;
    using Xunit.Abstractions;

    public class MySqlDockerDatabaseManager : MySqlDatabaseManager
    {
        private const string DockerImage = "mysql";
        private const string DockerTag = "5.6";
        private const string ContainerName = "sql-stream-store-tests-mysql";

        private readonly int _tcpPort;
        private readonly DockerContainer _mysqlContainer;

        public override string ConnectionString => ConnectionStringBuilder.ConnectionString;

        private MySqlConnectionStringBuilder ConnectionStringBuilder => new MySqlConnectionStringBuilder
        {
            Database = DatabaseName,
            Port = (uint) _tcpPort,
            UserID = "root",
            Pooling = false,
            MaximumPoolSize = 20
        };

        public MySqlDockerDatabaseManager(
            ITestOutputHelper testOutputHelper,
            string databaseName,
            int tcpPort = 3306)
            : base(testOutputHelper, databaseName)
        {
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

        public override async Task CreateDatabase(CancellationToken cancellationToken = default)
        {
            await _mysqlContainer.TryStart(cancellationToken).WithTimeout(60 * 1000 * 3);

            await base.CreateDatabase(cancellationToken);
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
                TestOutputHelper.WriteLine(ex.Message);
            }

            return false;
        }
    }
}