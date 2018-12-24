namespace SqlStreamStore.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore;
    using SqlStreamStore.Infrastructure;
    using Xunit.Abstractions;

    public class PostgresDockerDatabaseManager : PostgresDatabaseManager
    {
        private const string DockerImage = "postgres";
        private const string DockerTag = "10.4-alpine";
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

        public PostgresDockerDatabaseManager(
            ITestOutputHelper testOutputHelper, 
            string databaseName,
            int tcpPort = 5432)
            : base(testOutputHelper, databaseName)
        {
            _tcpPort = tcpPort;
            _postgresContainer = new DockerContainer(
                DockerImage,
                DockerTag,
                HealthCheck,
                new Dictionary<int, int>
                {
                    [tcpPort] = tcpPort
                })
            {
                ContainerName = ContainerName,
                Env = new[] { @"MAX_CONNECTIONS=500" }
            };
        }

        public override async Task CreateDatabase(CancellationToken cancellationToken = default)
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
                TestOutputHelper.WriteLine(ex.Message);
            }

            return false;
        }
    }
}