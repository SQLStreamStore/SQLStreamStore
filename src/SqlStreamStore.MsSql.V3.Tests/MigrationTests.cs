namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class MigrationTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public MigrationTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task Can_migrate()
        {
            // Set up an old schema + data.
            var schema = "baz";
            var v2Fixture = new MsSqlStreamStoreFixture(schema, deleteDatabaseOnDispose: false);
            var v2Store = await v2Fixture.GetMsSqlStreamStore();
            await v2Store.AppendToStream("stream-1",
                ExpectedVersion.NoStream,
                StreamStoreAcceptanceTests.CreateNewStreamMessages(1, 2, 3));
            await v2Store.AppendToStream("stream-2",
                ExpectedVersion.NoStream,
                StreamStoreAcceptanceTests.CreateNewStreamMessages(1, 2, 3));

            await v2Store.SetStreamMetadata("stream-1", ExpectedVersion.Any, maxAge: 10, maxCount: 20);
            v2Store.Dispose();
            v2Fixture.Dispose();

            var settings = new MsSqlStreamStoreV3Settings(v2Fixture.ConnectionString)
            {
                Schema = schema,
            };

            var v3Store = new MsSqlStreamStoreV3(settings);

            var checkSchemaResult = await v3Store.CheckSchema();
            checkSchemaResult.IsMatch().ShouldBeFalse();

            var progress = new Progress<MigrateProgress>();
            progress.ProgressChanged += (_, migrateProgress) =>
                _testOutputHelper.WriteLine($"Migration stage complete: {migrateProgress.Stage}");

            await v3Store.Migrate(progress, CancellationToken.None);

            checkSchemaResult = await v3Store.CheckSchema();
            checkSchemaResult.IsMatch().ShouldBeTrue();

            v3Store.Dispose();
        }
    }
}