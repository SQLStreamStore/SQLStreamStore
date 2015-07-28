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
        public async Task Read_forwards_to_the_end_should_return_a_valid_Checkpoint()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3, 4, 5, 6));

                    // read to the end of the stream
                    var allEventsPage = await eventStore.ReadAll(Checkpoint.Start, 4);
                    while (!allEventsPage.IsEnd)
                    {
                        allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, 10);
                    }

                    allEventsPage.IsEnd.Should().BeTrue();

                    Checkpoint currentCheckpoint = allEventsPage.NextCheckpoint;
                    currentCheckpoint.Should().NotBeNull();

                    // read end of stream again, should be empty, should return same checkpoint
                    allEventsPage = await eventStore.ReadAll(currentCheckpoint, 10);
                    allEventsPage.StreamEvents.Should().BeEmpty();
                    allEventsPage.IsEnd.Should().BeTrue();
                    allEventsPage.NextCheckpoint.Should().NotBeNull();
                    allEventsPage.NextCheckpoint.Should().Be(currentCheckpoint.Value);

                    // append some events then read again from the saved checkpoint, the next checkpoint should have moved
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.Any, CreateNewStreamEvents(7, 8, 9));
                    allEventsPage = await eventStore.ReadAll(currentCheckpoint, 10);
                    allEventsPage.IsEnd.Should().BeTrue();
                    allEventsPage.NextCheckpoint.Should().NotBeNull();
                    allEventsPage.NextCheckpoint.Should().NotBe(currentCheckpoint.Value);
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