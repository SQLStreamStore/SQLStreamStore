namespace SqlStreamStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;
    using SqlStreamStore.TestUtils.Postgres;
    using Xunit;
    using Xunit.Abstractions;

    public class PostgresStreamStoreV3FixturePool : IAsyncLifetime
    {
        private readonly ConcurrentDictionary<string, ConcurrentQueue<PostgresStreamStoreFixture>> _fixturePoolBySchema
            = new ConcurrentDictionary<string, ConcurrentQueue<PostgresStreamStoreFixture>>();

        public async Task<PostgresStreamStoreFixture> Get(
            ITestOutputHelper outputHelper,
            string schema = "dbo")
        {
            var fixturePool = _fixturePoolBySchema.GetOrAdd(
                schema,
                _ => new ConcurrentQueue<PostgresStreamStoreFixture>());

            if (!fixturePool.TryDequeue(out var fixture))
            {
                var databaseName = $"test_{Guid.NewGuid():n}";
                var dockerInstance = new PostgresContainer(schema, databaseName);
                await dockerInstance.Start();
                await dockerInstance.CreateDatabase();

                fixture = new PostgresStreamStoreFixture(
                    schema,
                    dockerInstance,
                    databaseName,
                    () => fixturePool.Enqueue(fixture));

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