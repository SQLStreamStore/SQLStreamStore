namespace SqlStreamStore
{
    using System;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Server;
    using SqlStreamStore.Infrastructure;
    using Xunit.Abstractions;

    public class MySqlStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        private static int s_nextPort = MySqlServer.DefaultPort;

        private readonly ITestOutputHelper _testOutputHelper;
        private readonly string _databaseName;
        private MySqlServer _localInstance;

        public MySqlStreamStoreFixture(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

            _databaseName = $"StreamStoreTests-{Guid.NewGuid():n}";
        }

        public override async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            var streamStore = new MySqlStreamStore(new MySqlStreamStoreSettings(_localInstance?.GetConnectionString(_databaseName))
            {
                GetUtcNow = () => GetUtcNow()
            });

            await streamStore.CreateDatabase();

            return streamStore;
        }

        public async Task<MySqlStreamStore> GetUninitializedStreamStore()
        {
            await CreateDatabase();

            return new MySqlStreamStore(new MySqlStreamStoreSettings(_localInstance?.GetConnectionString(_databaseName))
            {
                GetUtcNow = () => GetUtcNow()
            });
        }

        public async Task<MySqlStreamStore> GetMySqlStreamStore()
        {
            await CreateDatabase();

            var settings = new MySqlStreamStoreSettings(_localInstance?.GetConnectionString(_databaseName))
            {
                GetUtcNow = () => GetUtcNow()
            };

            var store = new MySqlStreamStore(settings);
            await store.CreateDatabase();

            return store;
        }

        public override void Dispose() => _localInstance?.Dispose();

        public override long MinPosition => 1;

        private async Task CreateDatabase()
        {
            _localInstance = new MySqlServer(_testOutputHelper, await GetNextAvailablePort());

            await _localInstance.Start();
            using(var connection = _localInstance.CreateConnection())
            {
                await connection.OpenAsync().NotOnCapturedContext();
                var createDatabase = $"CREATE DATABASE `{_databaseName}`";
                using(var command = new MySqlCommand(createDatabase, connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private static async Task<int> GetNextAvailablePort()
        {
            while(true)
            {
                using(TcpClient tcpClient = new TcpClient())
                {
                    var nextPort = Interlocked.Increment(ref s_nextPort) - 1;
                    try
                    {
                        await tcpClient.ConnectAsync(IPAddress.Loopback, nextPort);
                        await Task.Delay(100);
                    }
                    catch(SocketException)
                    {
                        return nextPort;
                    }
                }
            }
        }
    }
}