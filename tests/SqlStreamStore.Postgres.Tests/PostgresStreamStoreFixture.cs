namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Postgres;
    using Xunit.Abstractions;

    public class PostgresStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public string ConnectionString => _databaseManager.ConnectionString;
        private readonly string _schema;
        private readonly PostgresDatabaseManager _databaseManager;

        public PostgresStreamStoreFixture(string schema)
            : this(schema, new ConsoleTestoutputHelper())
        { }

        public PostgresStreamStoreFixture(string schema, ITestOutputHelper testOutputHelper)
        {
            _schema = schema;

            _databaseManager = new PostgresDockerDatabaseManager(testOutputHelper, $"test_{Guid.NewGuid():n}");
        }

        public PostgresStreamStoreFixture(string schema, string connectionString)
        {
            _schema = schema;

            _databaseManager = new PostgresServerDatabaseManager(
                new ConsoleTestoutputHelper(),
                $"test_{Guid.NewGuid():n}",
                connectionString);
        }

        public override long MinPosition => 0;

        public override int MaxSubscriptionCount => 90;

        public override async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow(),
                ScavengeAsynchronously = false
            };

            var store = new PostgresStreamStore(settings);

            await store.CreateSchemaIfNotExists();

            return store;
        }

        public async Task<IStreamStore> GetStreamStore(string schema)
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow()
            };
            var store = new PostgresStreamStore(settings);

            await store.CreateSchemaIfNotExists();

            return store;
        }

        public async Task<PostgresStreamStore> GetPostgresStreamStore(bool scavengeAsynchronously = false)
        {
            var store = await GetUninitializedPostgresStreamStore(scavengeAsynchronously);

            await store.CreateSchemaIfNotExists();

            return store;
        }

        public async Task<PostgresStreamStore> GetUninitializedPostgresStreamStore(bool scavengeAsynchronously = false)
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow(),
                ScavengeAsynchronously = scavengeAsynchronously
            };

            return new PostgresStreamStore(settings);
        }

        public override void Dispose()
        {
            _databaseManager?.Dispose();
        }

        public Task CreateDatabase() => _databaseManager.CreateDatabase();

        private class ConsoleTestoutputHelper : ITestOutputHelper
        {
            public void WriteLine(string message) => Console.Write(message);

            public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);
        }
    }
}