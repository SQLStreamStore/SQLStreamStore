namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.MySql;
    using Xunit.Abstractions;

    public class MySqlStreamStoreFixture : IStreamStoreFixture
    {
        private readonly string _databaseName;
        private readonly bool _createSchema;
        private readonly MySqlDatabaseManager _databaseManager;

        public string DatabaseName => _databaseName;

        private MySqlStreamStoreFixture(
            ITestOutputHelper testOutputHelper,
            bool createSchema)
        {
            _databaseName = $"test_{Guid.NewGuid():n}";
            _createSchema = createSchema;
            _databaseManager = new MySqlDockerDatabaseManager(
                testOutputHelper,
                _databaseName);
        }

        IStreamStore IStreamStoreFixture.Store => Store;

        public MySqlStreamStore Store { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public string ConnectionString => _databaseManager.ConnectionString;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 100;

        private async Task Init()
        {
            await _databaseManager.CreateDatabase();
            var settings = new MySqlStreamStoreSettings(ConnectionString)
            {
                GetUtcNow = () => GetUtcNow(),
                ScavengeAsynchronously = false
            };

            Store = new MySqlStreamStore(settings);

            if(_createSchema)
            {
                await Store.CreateSchemaIfNotExists();
            }
        }

        public static async Task<MySqlStreamStoreFixture> Create(
            ITestOutputHelper testOutputHelper = null,
            bool createSchema = true)
        {
            var fixture = new MySqlStreamStoreFixture(testOutputHelper, createSchema);
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