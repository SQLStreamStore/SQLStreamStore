namespace SqlStreamStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;
    using SqlStreamStore.TestUtils.MySql;
    using Xunit;
    using Xunit.Abstractions;

    public class MySqlStreamStoreFixturePool : IAsyncLifetime
    {
        private readonly ConcurrentQueue<MySqlStreamStoreFixture> _fixturePool
            = new ConcurrentQueue<MySqlStreamStoreFixture>();

        public async Task<MySqlStreamStoreFixture> Get(ITestOutputHelper outputHelper)
        {
            if (!_fixturePool.TryDequeue(out var fixture))
            {
                var dbUniqueName = (DateTime.UtcNow - DateTime.UnixEpoch).TotalMilliseconds;
                var databaseName = $"sss-v3-{dbUniqueName}";
                var dockerInstance = new MySqlContainer(databaseName);
                await dockerInstance.Start();
                await dockerInstance.CreateDatabase();

                fixture = new MySqlStreamStoreFixture(
                    dockerInstance,
                    databaseName,
                    onDispose:() => _fixturePool.Enqueue(fixture));

                outputHelper.WriteLine($"Using new fixture with db {databaseName}");
            }
            else
            {
                outputHelper.WriteLine($"Using pooled fixture with db {fixture.DatabaseName}");
            }

            await fixture.Prepare();

            return fixture;
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }
    }
}