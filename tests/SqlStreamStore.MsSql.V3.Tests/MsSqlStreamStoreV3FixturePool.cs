namespace SqlStreamStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;
    using SqlStreamStore.TestUtils.MsSql;
    using Xunit;
    using Xunit.Abstractions;

    public class MsSqlStreamStoreV3FixturePool : IAsyncLifetime
    {
        private readonly ConcurrentDictionary<string, ConcurrentQueue<MsSqlStreamStoreV3Fixture>> _fixturePoolBySchema
            = new ConcurrentDictionary<string, ConcurrentQueue<MsSqlStreamStoreV3Fixture>>();

        public async Task<MsSqlStreamStoreV3Fixture> Get(
            ITestOutputHelper outputHelper,
            string schema = "dbo")
        {
            var fixturePool = _fixturePoolBySchema.GetOrAdd(
                schema,
                _ => new ConcurrentQueue<MsSqlStreamStoreV3Fixture>());

            if (!fixturePool.TryDequeue(out var fixture))
            {
                var databaseName = $"sss-v3-{Guid.NewGuid():N}";
                var dockerInstance = new SqlServerContainer(databaseName);
                await dockerInstance.Start();
                await dockerInstance.CreateDatabase();

                fixture = new MsSqlStreamStoreV3Fixture(
                    schema,
                    dockerInstance,
                    databaseName,
                    onDispose:() => fixturePool.Enqueue(fixture));

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