namespace SqlStreamStore.TestUtils.Postgres
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    //using Ductus.FluentDocker.Builders;
    using Ductus.FluentDocker.Model.Containers;
    using Ductus.FluentDocker.Services;
    using Npgsql;
    using Polly;

    public class PostgresContainer : PostgresDatabaseManager
    {
        private readonly IContainerService _containerService;
        private const string Image = "postgres:10.4-alpine";
        private const string ContainerName = "sql-stream-store-tests-postgres";

        private readonly int _port;
        //public override string ConnectionString => ConnectionStringBuilder.ConnectionString;


        public PostgresContainer(string schema, string databaseName, float cpu = float.MinValue)
            : base(databaseName)
        {
            CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("en-US");

            var hosts = new Hosts().Discover();
            var host = hosts.FirstOrDefault(x => x.IsNative) ?? hosts.FirstOrDefault(x => x.Name == "default");


            var b = host.GetRunningContainers().Select(x => x.GetConfiguration(true))
                .Select(x => x.NetworkSettings.Ports).SelectMany(x => x.Values).Where(x => x is not null && x.Any()).SelectMany(x => x).Select(x => x.Port).ToList();

            var possiblePort = 5432;
            while (b.Contains(possiblePort))
            {
                possiblePort++;
            }
            _port = possiblePort;

            _containerService = host.Create(Image, false, new ContainerCreateParams
            {
                Cpus = cpu,
                Name = $"{schema}-{ContainerName}",
                PortMappings = new[] { $"{_port}:5432" }

            }, false, true, command: "-N 500");


            //_containerService = new Builder()
            //   .UseContainer()
            //   .WithName(ContainerName)
            //   .UseImage(Image)
            //   .KeepRunning()
            //   .ReuseIfExists()
            //   //.ExposePort(Port, Port)
            //   .Command("-N", "500")
            //   .WithParentCGroup()
            //   .Build();

            //var c = _containerService.GetConfiguration(true);
            //var d = c1.GetConfiguration(true);
        }

        public async Task Start(CancellationToken cancellationToken = default)
        {
            _containerService.Start();

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
                ApplicationName = applicationName
            }.ConnectionString;
        }

        private NpgsqlConnectionStringBuilder ConnectionStringBuilder => new NpgsqlConnectionStringBuilder
        {
            Database = DatabaseName,
            Password = Environment.OSVersion.IsWindows()
                ? "password"
                : null,
            Port = _port,
            Username = "postgres",
            Host = "localhost",
            Pooling = true,
            MaxPoolSize = 1024,
            Timeout = 300,
            IncludeErrorDetails = false
        };
    }
}
