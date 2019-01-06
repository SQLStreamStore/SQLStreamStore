namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Postgres;
    using Xunit.Abstractions;

    public class PostgresStreamStoreFixture2 : IStreamStoreFixture
    {
        private readonly PostgresStreamStoreFixture _fixture;

        private PostgresStreamStoreFixture2(
            PostgresStreamStoreFixture fixture,
            PostgresStreamStore store)
        {
            _fixture = fixture;
            Store = store;
        }

        public static async Task<PostgresStreamStoreFixture2> Create(
            string schema = "dbo",
            ITestOutputHelper testOutputHelper = null)
        {
            var innerFixture = new PostgresStreamStoreFixture(schema, testOutputHelper);
            var store = await innerFixture.GetPostgresStreamStore();
            return new PostgresStreamStoreFixture2(innerFixture, store);
        }

        public static async Task<PostgresStreamStoreFixture2> CreateUninitialized(
            string schema = "dbo",
            ITestOutputHelper testOutputHelper = null)
        {
            var innerFixture = new PostgresStreamStoreFixture(schema, testOutputHelper);
            var store = await innerFixture.GetUninitializedPostgresStreamStore();
            return new PostgresStreamStoreFixture2(innerFixture, store);
        }

        public void Dispose()
        {
            Store.Dispose();
            _fixture.Dispose();
        }

        IStreamStore IStreamStoreFixture.Store => Store;

        public PostgresStreamStore Store { get; }

        public GetUtcNow GetUtcNow
        {
            get => _fixture.GetUtcNow;
            set => _fixture.GetUtcNow = value;
        }

        public string ConnectionString => _fixture.ConnectionString;
    }

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