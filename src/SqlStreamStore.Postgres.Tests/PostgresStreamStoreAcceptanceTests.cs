namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class PostgresStreamStoreAcceptanceTests : StreamStoreAcceptanceTests
    {
        public PostgresStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override StreamStoreAcceptanceTestFixture GetFixture()
            => new PostgresStreamStoreFixture("foo", TestOutputHelper);

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using(var fixture = new PostgresStreamStoreFixture("dbo", TestOutputHelper))
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
            using (var fixture = new PostgresStreamStoreFixture("dbo", TestOutputHelper))
            {
                using (var store = await fixture.GetPostgresStreamStore())
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

        [Fact]
        public async Task when_try_scavenge_fails_returns_negative_one()
        {
            using(var fixture = new PostgresStreamStoreFixture("dbo", TestOutputHelper))
            {
                using(var store = await fixture.GetPostgresStreamStore())
                {
                    var cts = new CancellationTokenSource();
                    
                    cts.Cancel();

                    var result = await store.TryScavenge(new StreamIdInfo("stream-1"), 10, cts.Token);
                    
                    result.ShouldBe(-1);
                }
            }
        }
    }
}