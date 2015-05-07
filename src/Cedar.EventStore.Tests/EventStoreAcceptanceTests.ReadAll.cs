namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Xunit;

    public abstract partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_read_all()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamEvents(4, 5, 6));
                    var expectedEvents = new[]
                    {
                        ExpectedStreamEvent("stream-1", 1, 0),
                        ExpectedStreamEvent("stream-1", 2, 1),
                        ExpectedStreamEvent("stream-1", 3, 2),
                        ExpectedStreamEvent("stream-2", 4, 0),
                        ExpectedStreamEvent("stream-2", 5, 1),
                        ExpectedStreamEvent("stream-2", 6, 2),
                    };

                    var allEventsPage = await eventStore.ReadAll(null, 10);
                    List<StreamEvent> streamEvents = new List<StreamEvent>(allEventsPage.StreamEvents);
                    int count = 0;
                    while(!allEventsPage.IsEnd && count <20) //should not take more than 20 iterations.
                    {
                        allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, 10);
                        streamEvents.AddRange(allEventsPage.StreamEvents);
                        count++;
                    }

                    count.Should().BeLessThan(20);
                    allEventsPage.Direction.Should().Be(ReadDirection.Forward);
                    allEventsPage.IsEnd.Should().BeTrue();

                    streamEvents.ShouldAllBeEquivalentTo(
                        expectedEvents,
                        options => options.Excluding(@event => @event.Checkpoint));
                }
            }
        }
    }
}