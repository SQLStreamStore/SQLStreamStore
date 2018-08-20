namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class MsSqlStreamStoreV3AcceptanceTests : StreamStoreAcceptanceTests
    {
        public MsSqlStreamStoreV3AcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override StreamStoreAcceptanceTestFixture GetFixture()
            => new MsSqlStreamStoreV3Fixture("foo");


        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using(var fixture = new MsSqlStreamStoreV3Fixture("dbo"))
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
            using (var fixture = new MsSqlStreamStoreV3Fixture("dbo"))
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

                    var streamCount = await store.GetMessageCount(streamId, new DateTime(2016, 1, 1, 0, 1, 0));

                    streamCount.ShouldBe(3); // The first 3
                }
            }
        }

        [Theory, InlineData("dbo"), InlineData("myschema")]
        public async Task Can_call_initialize_repeatably(string schema)
        {
            using(var fixture = new MsSqlStreamStoreV3Fixture(schema))
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
            using (var fixture = new MsSqlStreamStoreV3Fixture("dbo"))
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
            using (var fixture = new MsSqlStreamStoreV3Fixture("dbo"))
            {
                using (var store = await fixture.GetMsSqlStreamStore())
                {
                    var result = await store.CheckSchema();

                    result.ExpectedVersion.ShouldBe(3);
                    result.CurrentVersion.ShouldBe(3);
                    result.IsMatch().ShouldBeTrue();
                }
            }
        }

        [Fact]
        public async Task When_schema_is_not_created_then_should_be_indicated()
        {
            using (var fixture = new MsSqlStreamStoreV3Fixture("dbo"))
            {
                using (var store = await fixture.GetUninitializedStreamStore())
                {
                    var result = await store.CheckSchema();

                    result.ExpectedVersion.ShouldBe(3);
                    result.CurrentVersion.ShouldBe(0);
                    result.IsMatch().ShouldBeFalse();
                }
            }
        }
    }
}