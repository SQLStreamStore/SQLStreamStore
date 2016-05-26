namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;
    using Xunit.Abstractions;

    public partial class EventStoreAcceptanceTests
    {
        private EventStoreAcceptanceTestFixture GetFixture(string schema = "foo")
        {
            return new MsSqlEventStoreFixture(schema);
        }

        private IDisposable CaptureLogs(ITestOutputHelper testOutputHelper)
        {
            return LoggingHelper.Capture(testOutputHelper);
        }

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using(var fixture = new MsSqlEventStoreFixture("dbo"))
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
    }
}