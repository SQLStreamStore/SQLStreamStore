namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Xunit;

    public abstract partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_read_to_end_of_allstream()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    var lastAllEventPage = await ReadAllStreamToEnd(eventStore);

                    lastAllEventPage.StreamEvents.Should().BeEmpty();
                    lastAllEventPage.IsEnd.Should().BeTrue();
                    lastAllEventPage.NextCheckpoint.Should().Be(lastAllEventPage.FromCheckpoint);
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
                    await eventStore.AppendToStream("stream-1", 2, CreateNewStreamEvents(1, 2, 3));

                    var lastAllEventPage = await ReadAllStreamToEnd(eventStore);

                    lastAllEventPage.StreamEvents.Should().BeEmpty();
                    lastAllEventPage.IsEnd.Should().BeTrue();
                    lastAllEventPage.NextCheckpoint.Should().Be(lastAllEventPage.NextCheckpoint);
                }
            }
        }

        private async Task<AllEventsPage> ReadAllStreamToEnd(IEventStore eventStore)
        {
            int pageSize = 4, count = 0;
            var allEventsPage = await eventStore.ReadAll(Checkpoint.Start, pageSize);
            LogAllEventsPage(allEventsPage);
            while (!allEventsPage.IsEnd && count < 10)
            {
                _testOutputHelper.WriteLine(String.Format("Loop {0}", count));
                allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, pageSize);
                LogAllEventsPage(allEventsPage);
                count++;
            }
            return allEventsPage;
        }

        private void LogAllEventsPage(AllEventsPage allEventsPage)
        {
            _testOutputHelper.WriteLine(String.Format("FromCheckpoint     = {0}", allEventsPage.FromCheckpoint));
            _testOutputHelper.WriteLine(String.Format("NextCheckpoint     = {0}", allEventsPage.NextCheckpoint));
            _testOutputHelper.WriteLine(String.Format("IsEnd              = {0}", allEventsPage.IsEnd));
            _testOutputHelper.WriteLine(String.Format("StreamEvents.Count = {0}", allEventsPage.StreamEvents.Count));
            _testOutputHelper.WriteLine("");
        }
    }
}