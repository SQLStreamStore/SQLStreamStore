namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class MsSqlStreamStoreAcceptanceTests : StreamStoreAcceptanceTests
    {
        public MsSqlStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        protected override StreamStoreAcceptanceTestFixture GetFixture()
        {
            string schema = "foo";
            return new MsSqlStreamStoreFixture(schema);
        }

        protected override IDisposable CaptureLogs(ITestOutputHelper testOutputHelper)
        {
            return LoggingHelper.Capture(testOutputHelper);
        }

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using(var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using(var dboStore = await fixture.GetStreamStore())
                {
                    using(var barStore = await fixture.GetStreamStore("bar"))
                    {
                        await dboStore.AppendToStream("stream-1",
                                ExpectedVersion.NoStream,
                                CreateNewStreamMessages(1, 2));
                        await barStore.AppendToStream("stream-1",
                                ExpectedVersion.NoStream,
                                CreateNewStreamMessages(1, 2));

                        var dboHeadPosition = await dboStore.ReadHeadPosition();
                        var fooHeadPosition = await dboStore.ReadHeadPosition();

                        dboHeadPosition.ShouldBe(1);
                        fooHeadPosition.ShouldBe(1);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_get_stream_message_count_with_created_before_date()
        {
            using (var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using (var store = await fixture.GetMsSqlStreamStore())
                {
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
        }

        [Theory, InlineData("dbo"), InlineData("myschema")]
        public async Task Can_call_initialize_repeatably(string schema)
        {
            using(var fixture = new MsSqlStreamStoreFixture(schema))
            {
                using(var store = await fixture.GetMsSqlStreamStore())
                {
                    await store.CreateSchema();
                    await store.CreateSchema();
                }
            }
        }

        [Fact]
        public async Task Can_drop_all()
        {
            using (var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using (var store = await fixture.GetMsSqlStreamStore())
                {
                    await store.DropAll();
                }
            }
        }

        [Fact]
        public async Task Can_check_schema()
        {
            using (var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using (var store = await fixture.GetMsSqlStreamStore())
                {
                    var result = await store.CheckSchema();

                    result.ExpectedVersion.ShouldBe(2);
                    result.CurrentVersion.ShouldBe(2);
                    result.IsMatch().ShouldBeTrue();
                }
            }
        }

        [Fact]
        public async Task When_schema_is_v1_then_should_not_match()
        {
            using (var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using (var store = await fixture.GetStreamStore_v1Schema())
                {
                    var result = await store.CheckSchema();

                    result.ExpectedVersion.ShouldBe(2);
                    result.CurrentVersion.ShouldBe(1);
                    result.IsMatch().ShouldBeFalse();
                }
            }
        }
    }
}