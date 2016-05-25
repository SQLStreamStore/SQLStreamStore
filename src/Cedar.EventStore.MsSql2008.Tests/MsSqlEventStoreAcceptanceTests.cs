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

        [Fact]
        public async Task MainAsync()
        {
            using (var fixture = new MsSqlEventStoreFixture("dbo"))
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    var cts = new CancellationTokenSource();
                    var tasks = new List<Task>();

                    for(int i = 0; i < Environment.ProcessorCount; i++)
                    {
                        var task = Task.Run(async () =>
                        {
                            int eventNumber = i*10000000;
                            while(!cts.IsCancellationRequested)
                            {
                                try
                                {
                                    await eventStore.AppendToStream("stream-1",
                                        ExpectedVersion.Any,
                                        CreateNewStreamEvents(eventNumber, eventNumber + 1),
                                        cts.Token);
                                    eventNumber += 2;
                                }
                                catch(Exception ex) when(!(ex is TaskCanceledException))
                                {
                                    throw;
                                }
                            }
                        }, cts.Token);
                        tasks.Add(task);
                    }
                    await Task.WhenAll(tasks);
                }
            }
        }
    }
}