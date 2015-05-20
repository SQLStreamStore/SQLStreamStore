namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Xunit;

    public abstract partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_read_all_forwards()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamEvents(4, 5, 6));
                    var expectedEvents = new[]
                    {
                        ExpectedStreamEvent("stream-1", 1, 0, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-1", 2, 1, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-1", 3, 2, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-2", 4, 0, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-2", 5, 1, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-2", 6, 2, fixture.GetUtcNow().UtcDateTime)
                    };

                    var allEventsPage = await eventStore.ReadAll(Checkpoint.Start, 4);
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
                         options =>
                         {
                             options.Excluding(streamEvent => streamEvent.Checkpoint);
                             options.Excluding(streamEvent => streamEvent.Created);
                             return options;
                         });
                }
            }
        }

        [Fact]
        public async Task Can_read_all_backward()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamEvents(4, 5, 6));
                    var expectedEvents = new[]
                    {
                        ExpectedStreamEvent("stream-1", 1, 0, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-1", 2, 1, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-1", 3, 2, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-2", 4, 0, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-2", 5, 1, fixture.GetUtcNow().UtcDateTime),
                        ExpectedStreamEvent("stream-2", 6, 2, fixture.GetUtcNow().UtcDateTime)
                    };

                    var allEventsPage = await eventStore.ReadAll(Checkpoint.End, 4, ReadDirection.Backward);
                    List<StreamEvent> streamEvents = new List<StreamEvent>(allEventsPage.StreamEvents);
                    int count = 0;
                    while (!allEventsPage.IsEnd && count < 20) //should not take more than 20 iterations.
                    {
                        allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, 10, ReadDirection.Backward);
                        streamEvents.AddRange(allEventsPage.StreamEvents);
                        count++;
                    }

                    count.Should().BeLessThan(20);
                    allEventsPage.Direction.Should().Be(ReadDirection.Backward);
                    allEventsPage.IsEnd.Should().BeTrue();

                    streamEvents.ShouldAllBeEquivalentTo(
                        expectedEvents,
                         options =>
                         {
                             options.Excluding(streamEvent => streamEvent.Checkpoint);
                             options.Excluding(streamEvent => streamEvent.Created);
                             return options;
                         });
                }
            }
        }
    }
}