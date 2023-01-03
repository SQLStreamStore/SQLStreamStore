namespace LoadTests
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore;
    using SqlStreamStore.TestUtils.Postgres;
    using Xunit.Abstractions;

    public class PostgresStreamStoreDb : IDisposable
    {
        public string ConnectionString => _databaseManager.ConnectionString;
        private readonly string _schema;
        private readonly PostgresContainer _databaseManager;

        public PostgresStreamStoreDb(string schema)
            : this(schema, new ConsoleTestoutputHelper())
        { }

        public PostgresStreamStoreDb(string schema, ITestOutputHelper testOutputHelper)
        {
            _schema = schema;
            _databaseManager = new PostgresContainer($"test_{Guid.NewGuid():n}");
        }

        public PostgresStreamStoreDb(string schema, string connectionString)
        {
            _schema = schema;
            _databaseManager = new PostgresContainer($"test_{Guid.NewGuid():n}");
        }

        public async Task<PostgresStreamStore> GetPostgresStreamStore(GapHandlingSettings gapHandlingSettings, bool scavengeAsynchronously = false)
        {
            var store = await GetUninitializedPostgresStreamStore(gapHandlingSettings, scavengeAsynchronously);

            await store.CreateSchemaIfNotExists();

            return store;
        }

        public async Task<PostgresStreamStore> GetUninitializedPostgresStreamStore(GapHandlingSettings gapHandlingSettings, bool scavengeAsynchronously = false)
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString, gapHandlingSettings)
            {
                Schema = _schema,
                ScavengeAsynchronously = scavengeAsynchronously
            };

            return new PostgresStreamStore(settings);
        }

        public void Dispose()
        {
            _databaseManager?.Dispose();
        }

        public Task Start() => _databaseManager.Start();
        public Task CreateDatabase() => _databaseManager.CreateDatabase();

        private class ConsoleTestoutputHelper : ITestOutputHelper
        {
            public void WriteLine(string message) => Console.Write(message);

            public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);
        }
    }
}