namespace LoadTests
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.TestUtils.MySql;
    using SqlStreamStore.V1;
    using Xunit.Abstractions;

    public class MySqlStreamStoreDb : IDisposable
    {
        public string ConnectionString => _databaseManager.ConnectionString;
        private readonly MySqlDatabaseManager _databaseManager;

        public MySqlStreamStoreDb()
            : this(new ConsoleTestoutputHelper())
        {
            
        }
        public MySqlStreamStoreDb(ITestOutputHelper testOutputHelper)
        {
            _databaseManager = new MySqlDockerDatabaseManager(
                testOutputHelper, 
                $"test_{Guid.NewGuid():n}");
        }

        public MySqlStreamStoreDb(string connectionString)
        {
            _databaseManager = new MySqlServerDatabaseManager(
                new ConsoleTestoutputHelper(),
                $"test_{Guid.NewGuid():n}",
                connectionString);
        }

        public async Task<MySqlStreamStore> GetMySqlStreamStore(bool scavengeAsynchronously = false)
        {
            var store = await GetUninitializedMySqlStreamStore(scavengeAsynchronously);

            await store.CreateSchemaIfNotExists();

            return store;
        }

        public async Task<MySqlStreamStore> GetUninitializedMySqlStreamStore(bool scavengeAsynchronously = false)
        {
            await CreateDatabase();

            var settings = new MySqlStreamStoreSettings(ConnectionString)
            {
                ScavengeAsynchronously = scavengeAsynchronously
            };

            return new MySqlStreamStore(settings);
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