namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public async Task Can_read_to_end_of_allstream(int numberOfStreams)
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    for(int i = 0; i < numberOfStreams; i++)
                    {
                        await eventStore.AppendToStream($"stream-{i}", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    }
                    
                    var lastAllEventPage = await ReadAllStreamToEnd(eventStore);

                    lastAllEventPage.IsEnd.ShouldBeTrue();
                }
            }
        }

        [Fact]
        public async Task When_read_to_end_of_allstream_then_append_then_can_read_to_end_of_allstream()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await ReadAllStreamToEnd(eventStore);
                    await eventStore.AppendToStream("stream-1", 2, CreateNewStreamEvents(4, 5, 6));

                    var lastAllEventPage = await ReadAllStreamToEnd(eventStore);

                    lastAllEventPage.IsEnd.ShouldBeTrue();
                    lastAllEventPage.NextCheckpoint.ShouldBe(lastAllEventPage.NextCheckpoint);
                }
            }
        }

        private async Task<AllEventsPage> ReadAllStreamToEnd(IEventStore eventStore)
        {
            int pageSize = 4, count = 0;
            var allEventsPage = await eventStore.ReadAll(Checkpoint.Start, pageSize);
            LogAllEventsPage(allEventsPage);
            while (!allEventsPage.IsEnd && count < 20)
            {
                _testOutputHelper.WriteLine($"Loop {count}");
                allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, pageSize);
                LogAllEventsPage(allEventsPage);
                count++;
            }
            return allEventsPage;
        }

        private void LogAllEventsPage(AllEventsPage allEventsPage)
        {
            _testOutputHelper.WriteLine($"FromCheckpoint     = {allEventsPage.FromCheckpoint}");
            _testOutputHelper.WriteLine($"NextCheckpoint     = {allEventsPage.NextCheckpoint}");
            _testOutputHelper.WriteLine($"IsEnd              = {allEventsPage.IsEnd}");
            _testOutputHelper.WriteLine($"StreamEvents.Count = {allEventsPage.StreamEvents.Count}");
            _testOutputHelper.WriteLine("");
        }
    }
}