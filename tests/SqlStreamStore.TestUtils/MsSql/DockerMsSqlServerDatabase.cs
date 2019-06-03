namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Polly;
    using SqlStreamStore.Infrastructure;

    public class DockerMsSqlServerDatabase
    {
        private readonly string _databaseName;
        private readonly DockerContainer _sqlServerContainer;
        private const string Password = "!Passw0rd";
        private const string Image = "mcr.microsoft.com/mssql/server";
        private const string Tag = "2017-CU14-ubuntu";
        private const int Port = 11433;

        public DockerMsSqlServerDatabase(string databaseName)
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
                Env = new[]
                {
                    "ACCEPT_EULA=Y",
                    $"SA_PASSWORD={Password}",
                    "LD_PRELOAD=/opt/nodirect_open.so"
                },
                DataDirectories = new[] { "/var/opt/mssql/data" },
                Volumes =
                {
                    [$"{AppDomain.CurrentDomain.BaseDirectory}/MsSql/nodirect_open.so"] = "/opt/nodirect_open.so"
                }
            };
        }

        public SqlConnection CreateConnection()
            => new SqlConnection(CreateConnectionStringBuilder().ConnectionString);

        public SqlConnectionStringBuilder CreateConnectionStringBuilder()
            => new SqlConnectionStringBuilder(
                $"server=localhost,{Port};User Id=sa;Password={Password};Initial Catalog=master");

        public async Task CreateDatabase(CancellationToken cancellationToken = default)
        {
            await _sqlServerContainer.TryStart(cancellationToken).WithTimeout(3 * 60 * 1000);

            var policy = Policy
                .Handle<SqlException>()
                .WaitAndRetryAsync(5, i => TimeSpan.FromSeconds(Math.Pow(2, i)));

            await policy.ExecuteAsync(async () =>
            {
                using(var connection = CreateConnection())
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                    using(var command = new SqlCommand($@"CREATE DATABASE [{_databaseName}]", connection))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }
                }
            }).NotOnCapturedContext();
        }

        private async Task<bool> HealthCheck(CancellationToken cancellationToken)
        {
            try
            {
                using(var connection = CreateConnection())
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                    return true;
                }
            }
            catch(Exception)
            { }

            return false;
        }
    }
}