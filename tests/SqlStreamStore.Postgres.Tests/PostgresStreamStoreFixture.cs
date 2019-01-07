namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Postgres;
    using Xunit.Abstractions;

    public class PostgresStreamStoreFixture : IStreamStoreFixture
    {
        private readonly string _schema;
        private readonly bool _createSchema;
        private readonly PostgresDatabaseManager _databaseManager;

        private PostgresStreamStoreFixture(
            string schema,
            ITestOutputHelper testOutputHelper,
            bool createSchema)
        {
            _schema = schema;
            _createSchema = createSchema;
            _databaseManager = new PostgresDockerDatabaseManager(
                testOutputHelper, 
                $"test_{Guid.NewGuid():n}");
        }

        IStreamStore IStreamStoreFixture.Store => Store;

        public PostgresStreamStore Store { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public string ConnectionString => _databaseManager.ConnectionString;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;

        private async Task Init()
        {
            await _databaseManager.CreateDatabase();
            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow(),
                ScavengeAsynchronously = false
            };

            Store = new PostgresStreamStore(settings);

            if(_createSchema)
            {
                await Store.CreateSchemaIfNotExists();
            }
        }

        public static async Task<PostgresStreamStoreFixture> Create(
            string schema = "dbo",
            ITestOutputHelper testOutputHelper = null,
            bool createSchema = true)
        {
            var fixture = new PostgresStreamStoreFixture(schema, testOutputHelper, createSchema);
            await fixture.Init();
            return fixture;
        }

        public void Dispose()
        {
            Store.Dispose();
            _databaseManager.Dispose();
        }
    }
}