namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class MigrationTests
    {
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

            // Migrate with V3 schema.
            var v3Fixture = new MsSqlStreamStoreV3Fixture(schema, databaseNameOverride: v2Fixture.DatabaseName);
            var v3Store = await v3Fixture.GetMsSqlStreamStore();

            var checkSchemaResult = await v3Store.CheckSchema();
            checkSchemaResult.IsMatch().ShouldBeFalse();

            await v3Store.Migrate(CancellationToken.None);

            checkSchemaResult = await v3Store.CheckSchema();
            checkSchemaResult.IsMatch().ShouldBeTrue();

            v3Store.Dispose();
            v3Fixture.Dispose();
        }
    }
}