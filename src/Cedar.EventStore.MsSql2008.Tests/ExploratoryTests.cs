/*namespace Cedar.EventStore
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Cedar.EventStore.Subscriptions;
    using Xunit;
    using Xunit.Abstractions;

    public class ExploratoryTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public ExploratoryTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task SqlDependencyExplorations()
        {
            using(var fixture = new MsSqlEventStoreFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    Func<int, int, Task> createStreams = async (count, interval) =>
                    {
                        var stopwatch = Stopwatch.StartNew();
                        for(int i = 0; i < count; i++)
                        {
                            var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEventType", "{}");
                            await eventStore.AppendToStream($"stream-{i}-1", ExpectedVersion.NoStream, newStreamEvent);
                        }

                        _testOutputHelper.WriteLine(stopwatch.ElapsedMilliseconds.ToString());
                    };

                    using (var watcher = new SqlEventsWatcher(fixture.ConnectionString, eventStore))
                    {
                        await watcher.Start();

                        await createStreams(500, 1);

                        await Task.Delay(500);
                    }
                }
            }
        }
    }
}*/