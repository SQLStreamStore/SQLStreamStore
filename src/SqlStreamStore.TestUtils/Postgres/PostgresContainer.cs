namespace SqlStreamStore.TestUtils.Postgres
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Ductus.FluentDocker.Extensions;
    using Ductus.FluentDocker.Model.Containers;
    using Ductus.FluentDocker.Services;
    using Ductus.FluentDocker.Services.Extensions;
    using Npgsql;
    using Polly;

    public class PostgresContainer : PostgresDatabaseManager
    {
        private readonly IContainerService _containerService;
        private const string Image = "postgres:10.4-alpine";
        private const string ContainerName = "sql-stream-store-tests-postgres";

        public PostgresContainer(string schema, string databaseName, float cpu = float.MinValue)
            : base(databaseName)
        {
            CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("en-US");

            var hosts = new Hosts().Discover();
            var host = hosts.FirstOrDefault(x => x.IsNative) ?? hosts.FirstOrDefault(x => x.Name == "default");
           

            _containerService = host.Create(Image, false, new ContainerCreateParams
            {
                Cpus = cpu,
                Name = $"{schema}-{ContainerName}",
                PortMappings = new[] { "5432" }

            }, false, true, command: "-N 500");


            //_containerService = new Builder()
            //   .UseContainer()
            //   .WithName(ContainerName)
            //   .UseImage(Image)
            //   .KeepRunning()
            //   .ReuseIfExists()
            //   .ExposePort(Port, Port)
            //   .Command("-N", "500")
            //   .WithParentCGroup()
            //   .Build();
        }

        public async Task Start(CancellationToken cancellationToken = default)
        {
            _containerService.Start();
            _containerService.WaitForRunning();

            await Policy
                .Handle<NpgsqlException>()
                .WaitAndRetryAsync(100, _ => TimeSpan.FromMilliseconds(500))
                .ExecuteAsync(async () =>
                {
                    using(var connection = new NpgsqlConnection(DefaultConnectionString))
                    {
                        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    }
                });
        }

        public override string GenerateConnectionString(string applicationName = "default")
        {
            
            return new NpgsqlConnectionStringBuilder(ConnectionStringBuilder.ConnectionString)
            {
                ApplicationName = applicationName,
                Port = _containerService.ToHostExposedEndpoint("5432/tcp").Port
            }.ConnectionString;
        }

        private NpgsqlConnectionStringBuilder ConnectionStringBuilder => new()
        {
            Database = DatabaseName,
            Password = Environment.OSVersion.IsWindows()
                ? "password"
                : null,
            Username = "postgres",
            Host = "localhost",
            Pooling = true,
            MaxPoolSize = 1024,
            Timeout = 300,
            IncludeErrorDetails = false
        };
    }
}
