namespace LoadTests
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore;
    using SqlStreamStore.Postgres;
    using Xunit.Abstractions;

    public class PostgresStreamStoreDb : IDisposable
    {
        public string ConnectionString => _databaseManager.ConnectionString;
        private readonly string _schema;
        private readonly PostgresDatabaseManager _databaseManager;

        public PostgresStreamStoreDb(string schema)
            : this(schema, new ConsoleTestoutputHelper())
        { }

        public PostgresStreamStoreDb(string schema, ITestOutputHelper testOutputHelper)
        {
            _schema = schema;

            _databaseManager = new PostgresDockerDatabaseManager(
                testOutputHelper, 
                $"test_{Guid.NewGuid():n}");
        }

        public PostgresStreamStoreDb(string schema, string connectionString)
        {
            _schema = schema;

            _databaseManager = new PostgresDockerDatabaseManager(
                new ConsoleTestoutputHelper(),
                $"test_{Guid.NewGuid():n}");
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
                ScavengeAsynchronously = scavengeAsynchronously
            };

            return new PostgresStreamStore(settings);
        }

        public void Dispose()
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