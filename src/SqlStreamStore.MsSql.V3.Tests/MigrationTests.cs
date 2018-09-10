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
            using(var fixture = new MsSqlStreamStoreV3Fixture(schema))
            {
                using(var v2Store = await fixture.GetMsSqlStreamStoreV2())
                {
                    await v2Store.AppendToStream("stream-1",
                        ExpectedVersion.NoStream,
                        StreamStoreAcceptanceTests.CreateNewStreamMessages(1, 2, 3));
                    await v2Store.AppendToStream("stream-2",
                        ExpectedVersion.NoStream,
                        StreamStoreAcceptanceTests.CreateNewStreamMessages(1, 2, 3));

                    await v2Store.SetStreamMetadata("stream-1", ExpectedVersion.Any, maxAge: 10, maxCount: 20);
                }

                var settings = new MsSqlStreamStoreV3Settings(fixture.ConnectionString)
                {
                    Schema = schema,
                };

                using(var v3Store = new MsSqlStreamStoreV3(settings))
                {
                    var checkSchemaResult = await v3Store.CheckSchema();
                    checkSchemaResult.IsMatch().ShouldBeFalse();

                    await v3Store.Migrate(CancellationToken.None);

                    checkSchemaResult = await v3Store.CheckSchema();
                    checkSchemaResult.IsMatch().ShouldBeTrue();
                }
            }
        }
    }
}