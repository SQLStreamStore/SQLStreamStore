namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public partial class StreamStoreAcceptanceTests
    {
        private StreamStoreAcceptanceTestFixture GetFixture(string schema = "foo")
        {
            return new MsSqlStreamStoreFixture(schema);
        }

        private IDisposable CaptureLogs(ITestOutputHelper testOutputHelper)
        {
            return LoggingHelper.Capture(testOutputHelper);
        }

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using(var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using(var dboEventStore = await fixture.GetEventStore())
                {
                    using(var barEventStore = await fixture.GetEventStore("bar"))
                    {
                        await dboEventStore.AppendToStream("stream-1",
                                ExpectedVersion.NoStream,
                                CreateNewStreamEvents(1, 2));
                        await barEventStore.AppendToStream("stream-1",
                                ExpectedVersion.NoStream,
                                CreateNewStreamEvents(1, 2));

                        var dboHeadCheckpoint = await dboEventStore.ReadHeadCheckpoint();
                        var fooHeadCheckpoint = await dboEventStore.ReadHeadCheckpoint();

                        dboHeadCheckpoint.ShouldBe(1);
                        fooHeadCheckpoint.ShouldBe(1);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_get_stream_event_count()
        {
            using (var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using (var store = await fixture.GetMsSqlEventStore())
                {
                    var streamId = "stream-1";
                    await store.AppendToStream(
                        streamId,
                        ExpectedVersion.NoStream,
                        CreateNewStreamEvents(1, 2, 3, 4, 5));

                    var streamCount = await store.GetStreamEventCount(streamId);

                    streamCount.ShouldBe(5);
                }
            }
        }

        [Fact]
        public async Task When_stream_does_not_exist_then_stream_event_count_should_be_zero()
        {
            using (var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using (var store = await fixture.GetMsSqlEventStore())
                {
                    var streamId = "stream-doesnotexist";

                    var streamCount = await store.GetStreamEventCount(streamId);

                    streamCount.ShouldBe(0);
                }
            }
        }

        [Fact]
        public async Task Can_get_stream_event_count_with_created_before_date()
        {
            using (var fixture = new MsSqlStreamStoreFixture("dbo"))
            {
                using (var store = await fixture.GetMsSqlEventStore())
                {
                    fixture.GetUtcNow = () => new DateTime(2016, 1, 1, 0, 0, 0);

                    var streamId = "stream-1";
                    await store.AppendToStream(
                        streamId,
                        ExpectedVersion.NoStream,
                        CreateNewStreamEvents(1, 2, 3));

                    fixture.GetUtcNow = () => new DateTime(2016, 1, 1, 0, 1, 0);

                    await store.AppendToStream(
                        streamId,
                        ExpectedVersion.Any,
                        CreateNewStreamEvents(4, 5, 6));

                    var streamCount = await store.GetStreamEventCount(streamId, new DateTime(2016, 1, 1, 0, 1, 0));

                    streamCount.ShouldBe(3); // The first 3
                }
            }
        }
    }
}