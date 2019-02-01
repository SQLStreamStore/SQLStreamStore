namespace SqlStreamStore
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class MsSqlStreamStoreAcceptanceTests : AcceptanceTests
    {
        public MsSqlStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override async Task<IStreamStoreFixture> CreateFixture()
            => await MsSqlStreamStoreFixture.Create("foo");

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using(var dboFixture = await MsSqlStreamStoreFixture.Create())
            {
                var dboStore = dboFixture.Store;

                using(var barFixture = await MsSqlStreamStoreFixture.Create("bar"))
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
        }

        [Fact]
        public async Task Can_get_stream_message_count_with_created_before_date()
        {
            using (var fixture = await MsSqlStreamStoreFixture.Create())
            {
                var store = fixture.Store;
                fixture.GetUtcNow = () => new DateTime(2016, 1, 1, 0, 0, 0);

                var streamId = "stream-1";
                await store.AppendToStream(
                    streamId,
                    ExpectedVersion.NoStream,
                    CreateNewStreamMessages(1, 2, 3));

                fixture.GetUtcNow = () => new DateTime(2016, 1, 1, 0, 1, 0);

                await store.AppendToStream(
                    streamId,
                    ExpectedVersion.Any,
                    CreateNewStreamMessages(4, 5, 6));

                var streamCount = await store.GetmessageCount(streamId, new DateTime(2016, 1, 1, 0, 1, 0));

                streamCount.ShouldBe(3); // The first 3
            }
        }

        [Theory, InlineData("dbo"), InlineData("myschema")]
        public async Task Can_call_initialize_repeatably(string schema)
        {
            using(var fixture = await MsSqlStreamStoreFixture.Create(schema))
            {
                await fixture.Store.CreateSchema();
                await fixture.Store.CreateSchema();
            }
        }

        [Fact]
        public async Task Can_drop_all()
        {
            using (var fixture = await MsSqlStreamStoreFixture.Create())
            {
                await fixture.Store.DropAll();
            }
        }

        [Fact]
        public async Task Can_check_schema()
        {
            using (var fixture = await MsSqlStreamStoreFixture.Create())
            {
                var result = await fixture.Store.CheckSchema();

                result.ExpectedVersion.ShouldBe(2);
                result.CurrentVersion.ShouldBe(2);
                result.IsMatch().ShouldBeTrue();
            }
        }

        [Fact]
        public async Task When_schema_is_v1_then_should_not_match()
        {
            using (var fixture = await MsSqlStreamStoreFixture.CreateWithV1Schema())
            {
                var result = await fixture.Store.CheckSchema();

                result.ExpectedVersion.ShouldBe(2);
                result.CurrentVersion.ShouldBe(1);
                result.IsMatch().ShouldBeFalse();
            }
        }

        [Fact]
        public async Task When_schema_is_not_created_then_should_be_indicated()
        {
            using (var fixture = await MsSqlStreamStoreFixture.Create(createSchema:false))
            {
                var result = await fixture.Store.CheckSchema();

                result.ExpectedVersion.ShouldBe(2);
                result.CurrentVersion.ShouldBe(0);
                result.IsMatch().ShouldBeFalse();
            }
        }

        [Fact]
        public void Can_export_database_creation_script()
        {
            string schema = "custom_schema";
            var store = new MsSqlStreamStore(new MsSqlStreamStoreSettings("server=.;database=sss")
            {
                Schema = schema,
            });

            var sqlScript = store.GetSchemaCreationScript();
            sqlScript.ShouldBe(new ScriptsV2.Scripts("custom_schema").CreateSchema);
        }

        [Fact]
        public async Task Time_taken_to_append_and_read_large_message_with_prefetch()
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
            var readStreamPage = await fixture.Store.ReadStreamForwards(streamId, StreamVersion.Start, prefetchJsonData:false, maxCount: 1);
            var jsonData = await readStreamPage.Messages[0].GetJsonData();
            TestOutputHelper.WriteLine($"Read: {stopwatch.Elapsed}");
        }
    }
}