namespace SqlStreamStore
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public abstract class AcceptanceTests : AcceptanceTestsBase
    {
        public AcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override async Task<IStreamStoreFixture> CreateFixture()
            => await MsSqlStreamStoreV3Fixture.Create("foo");

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            var dboStore = fixture.Store;

            using (var barFixture = await MsSqlStreamStoreV3Fixture.Create("bar"))
            {
                var barStore = barFixture.Store;

                await dboStore.AppendToStream("stream-1",
                    ExpectedVersion.NoStream,
                    CreateNewStreamMessages(1, 2));
                await barStore.AppendToStream("stream-1",
                    ExpectedVersion.NoStream,
                    CreateNewStreamMessages(1, 2));

                var dboHeadPosition = await dboStore.ReadHeadPosition();
                var barHeadPosition = await barStore.ReadHeadPosition();

                dboHeadPosition.ShouldBe(1);
                barHeadPosition.ShouldBe(1);
            }
        }

        [Theory, InlineData("dbo"), InlineData("myschema")]
        public async Task Can_call_initialize_repeatably(string schema)
        {
            using (var fixture = await MsSqlStreamStoreV3Fixture.Create(schema, createSchema:false))
            {
                await fixture.Store.CreateSchemaIfNotExists();
                await fixture.Store.CreateSchemaIfNotExists();
            }
        }

        [Fact]
        public async Task Can_drop_all()
        {
            using (var fixture = await MsSqlStreamStoreV3Fixture.Create())
            {
                await fixture.Store.DropAll();
            }
        }

        [Fact]
        public async Task Can_check_schema()
        {
            using (var fixture = await MsSqlStreamStoreV3Fixture.Create())
            {
                var result = await fixture.Store.CheckSchema();

                result.ExpectedVersion.ShouldBe(3);
                result.CurrentVersion.ShouldBe(3);
                result.IsMatch().ShouldBeTrue();
            }
        }

        [Fact]
        public async Task When_schema_is_not_created_then_should_be_indicated()
        {
            using (var fixture = await MsSqlStreamStoreV3Fixture.Create(createSchema: false))
            {
                var result = await fixture.Store.CheckSchema();

                result.ExpectedVersion.ShouldBe(3);
                result.CurrentVersion.ShouldBe(0);
                result.IsMatch().ShouldBeFalse();
            }
        }

        [Fact]
        public void Can_export_database_creation_script()
        {
            string schema = "custom_schema";
            var store = new MsSqlStreamStoreV3(new MsSqlStreamStoreV3Settings("server=.;database=sss")
            {
                Schema = schema,
            });


            var sqlScript = store.GetSchemaCreationScript();
            sqlScript.ShouldBe(new ScriptsV3.Scripts("custom_schema").CreateSchema);
        }

        [Fact]
        public async Task Time_taken_to_append_and_read_large_message()
        {
            var stopwatch = Stopwatch.StartNew();
            var streamId = "stream-large";
            var data = new string('a', 1024 * 1024 * 2);
            var newStreamMessage = new NewStreamMessage(Guid.NewGuid(), "foo", data);
            await fixture.Store.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage);
            TestOutputHelper.WriteLine($"Append: {stopwatch.Elapsed}");

            stopwatch.Restart();
            var readStreamPage = await fixture.Store.ReadStreamForwards(streamId, StreamVersion.Start, 1);
            var jsonData = await readStreamPage.Messages[0].GetJsonData();
            TestOutputHelper.WriteLine($"Read: {stopwatch.Elapsed}");
        }

        [Fact]
        public async Task Time_taken_to_append_and_read_large_message_without_prefetch()
        {
            var stopwatch = Stopwatch.StartNew();
            var streamId = "stream-large";
            var data = new string('a', 1024 * 1024 * 2);
            var newStreamMessage = new NewStreamMessage(Guid.NewGuid(), "foo", data);
            await fixture.Store.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage);
            TestOutputHelper.WriteLine($"Append: {stopwatch.Elapsed}");

            stopwatch.Restart();
            var readStreamPage = await fixture.Store.ReadStreamForwards(streamId, StreamVersion.Start, prefetchJsonData: false, maxCount: 1);
            var jsonData = await readStreamPage.Messages[0].GetJsonData();
            TestOutputHelper.WriteLine($"Read: {stopwatch.Elapsed}");
        }
    }
}