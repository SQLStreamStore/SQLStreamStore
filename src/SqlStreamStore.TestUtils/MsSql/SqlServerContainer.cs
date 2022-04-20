namespace SqlStreamStore.TestUtils.MsSql
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Ductus.FluentDocker.Builders;
    using Ductus.FluentDocker.Services;
    using Microsoft.Data.SqlClient;
    using Polly;

    public class SqlServerContainer
    {
        private readonly string _databaseName;
        private IContainerService _containerService;
        private const string ContainerName = "sql-stream-store-tests-mssql";
        private const string Password = "E@syP@ssw0rd";
        private const string Image = "mcr.microsoft.com/mssql/server:2017-latest";
        private const int HostPort = 11433;
        private const int ContainerPort = 1433;

        public SqlServerContainer(string databaseName)
        {
            _databaseName = databaseName;

            _containerService = new Builder()
                .UseContainer()
                .WithName(ContainerName)
                .UseImage(Image)
                .KeepRunning()
                .ReuseIfExists()
                .WithEnvironment("ACCEPT_EULA=Y", $"SA_PASSWORD={Password}")
                .ExposePort(HostPort, ContainerPort)
                .Build();
        }

        public SqlConnection CreateConnection()
            => new SqlConnection(CreateConnectionStringBuilder().ConnectionString);

        public SqlConnectionStringBuilder CreateConnectionStringBuilder()
            => new SqlConnectionStringBuilder($"server=localhost,{HostPort};User Id=sa;Password={Password};Initial Catalog=master;TrustServerCertificate=true");

        public async Task Start(CancellationToken cancellationToken = default)
        {
            _containerService.Start();

            await Policy
                .Handle<SqlException>()
                .WaitAndRetryAsync(30, _ => TimeSpan.FromMilliseconds(500))
                .ExecuteAsync(async () =>
                {
                    using (var connection = new SqlConnection(CreateConnectionStringBuilder().ConnectionString))
                    {
                        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    }
                });
        }

        public async Task CreateDatabase(CancellationToken cancellationToken = default)
        {
            var policy = Policy
                .Handle<SqlException>()
                .WaitAndRetryAsync(3, i => TimeSpan.FromSeconds(1));

            await policy.ExecuteAsync(async () =>
            {
                using(var connection = CreateConnection())
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                    var createCommand = $@"CREATE DATABASE [{_databaseName}]
ALTER DATABASE [{_databaseName}] SET SINGLE_USER
ALTER DATABASE [{_databaseName}] SET COMPATIBILITY_LEVEL=110
ALTER DATABASE [{_databaseName}] SET MULTI_USER";

                    using(var command = new SqlCommand(createCommand, connection))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            });
        }
    }
}
