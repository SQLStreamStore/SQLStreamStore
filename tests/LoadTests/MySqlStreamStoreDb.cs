namespace LoadTests
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore;
    using SqlStreamStore.TestUtils.MySql;
    using Xunit.Abstractions;

    public class MySqlStreamStoreDb
    {
        private string ConnectionString => _databaseManager.ConnectionString;
        private readonly MySqlContainer _databaseManager;

        public MySqlStreamStoreDb()
            : this(new ConsoleTestoutputHelper())
        {
            
        }
        public MySqlStreamStoreDb(ITestOutputHelper testOutputHelper)
        {
            _databaseManager = new MySqlContainer($"test_{Guid.NewGuid():n}");
        }

        public async Task<MySqlStreamStore> GetUninitializedMySqlStreamStore()
        {
            await CreateDatabase();

            var settings = new MySqlStreamStoreSettings(ConnectionString);

            return new MySqlStreamStore(settings);
        }

        public Task CreateDatabase() => _databaseManager.CreateDatabase();

        private class ConsoleTestoutputHelper : ITestOutputHelper
        {
            public void WriteLine(string message) => Console.Write(message);

            public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);
        }
    }
}