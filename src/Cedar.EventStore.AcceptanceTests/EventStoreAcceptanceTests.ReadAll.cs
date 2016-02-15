namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
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

                    count.ShouldBeLessThan(20);
                    allEventsPage.Direction.ShouldBe(ReadDirection.Forward);
                    allEventsPage.IsEnd.ShouldBeTrue();

                    for (int i = 0; i < streamEvents.Count; i++)
                    {
                        var streamEvent = streamEvents[i];
                        var expectedEvent = expectedEvents[i];

                        streamEvent.EventId.ShouldBe(expectedEvent.EventId);
                        streamEvent.JsonData.ShouldBe(expectedEvent.JsonData);
                        streamEvent.JsonMetadata.ShouldBe(expectedEvent.JsonMetadata);
                        streamEvent.StreamId.ShouldBe(expectedEvent.StreamId);
                        streamEvent.StreamVersion.ShouldBe(expectedEvent.StreamVersion);
                        streamEvent.Type.ShouldBe(expectedEvent.Type);

                        // We don't care about streamEvent.Checkpoint and streamEvent.Checkpoint
                        // as they are non-deterministic
                    }
                }
            }
        }

        [Fact]
        public async Task Can_read_all_backwards()
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
                    }.Reverse().ToArray();

                    var allEventsPage = await eventStore.ReadAll(Checkpoint.End, 4, ReadDirection.Backward);
                    List<StreamEvent> streamEvents = new List<StreamEvent>(allEventsPage.StreamEvents);
                    int count = 0;
                    while (!allEventsPage.IsEnd && count < 20) //should not take more than 20 iterations.
                    {
                        allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, 10, ReadDirection.Backward);
                        streamEvents.AddRange(allEventsPage.StreamEvents);
                        count++;
                    }

                    count.ShouldBeLessThan(20);
                    allEventsPage.Direction.ShouldBe(ReadDirection.Backward);
                    allEventsPage.IsEnd.ShouldBeTrue();

                    streamEvents.Count.ShouldBe(expectedEvents.Length);

                    for(int i = 0; i < streamEvents.Count; i++)
                    {
                        var streamEvent = streamEvents[i];
                        var expectedEvent = expectedEvents[i];

                        streamEvent.EventId.ShouldBe(expectedEvent.EventId);
                        streamEvent.JsonData.ShouldBe(expectedEvent.JsonData);
                        streamEvent.JsonMetadata.ShouldBe(expectedEvent.JsonMetadata);
                        streamEvent.StreamId.ShouldBe(expectedEvent.StreamId);
                        streamEvent.StreamVersion.ShouldBe(expectedEvent.StreamVersion);
                        streamEvent.Type.ShouldBe(expectedEvent.Type);

                        // We don't care about streamEvent.Checkpoint and streamEvent.Checkpoint
                        // as they are non-deterministic
                    }
                }
            }
        }

        [Theory]
        [InlineData(3, 0, 3, 3, 0, 3)]  // Read entire store
        [InlineData(3, 0, 4, 3, 0, 3)]  // Read entire store
        [InlineData(3, 0, 2, 2, 0, 2)]
        [InlineData(3, 1, 2, 2, 1, 3)]
        [InlineData(3, -1, 1, 1, 2, 3)] // -1 is Checkpoint.End
        [InlineData(3, 2, 1, 1, 2, 3)]
        [InlineData(3, 3, 1, 0, 3, 3)]
        public async Task When_read_all_forwards(
            int numberOfSeedEvents,
            int fromCheckpoint,
            int maxCount,
            int expectedCount,
            int expectedFromCheckpoint,
            int expectedNextCheckPoint)
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream(
                        "stream-1",
                        ExpectedVersion.NoStream,
                        CreateNewStreamEventSequence(1, numberOfSeedEvents));

                    var allEventsPage = await eventStore.ReadAll(fromCheckpoint, maxCount);

                    allEventsPage.StreamEvents.Length.ShouldBe(expectedCount);
                    allEventsPage.FromCheckpoint.ShouldBe(expectedFromCheckpoint);
                    allEventsPage.NextCheckpoint.ShouldBe(expectedNextCheckPoint);
                }
            }
        }

        [Theory]
        [InlineData(3, -1, 1, 1, 2, 1)] // -1 is Checkpoint.End
        [InlineData(3, 2, 1, 1, 2, 1)]
        [InlineData(3, 1, 1, 1, 1, 0)]
        [InlineData(3, 0, 1, 1, 0, 0)]
        [InlineData(3, -1, 3, 3, 2, 0)] // Read entire store
        [InlineData(3, -1, 4, 3, 2, 0)] // Read entire store
        [InlineData(0, -1, 0, 0, 0, 0)]
        public async Task When_read_all_backwards(
            int numberOfSeedEvents,
            int fromCheckpoint,
            int maxCount,
            int expectedCount,
            int expectedFromCheckpoint,
            int expectedNextCheckPoint)
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    if(numberOfSeedEvents > 0)
                    {
                        await eventStore.AppendToStream(
                            "stream-1",
                            ExpectedVersion.NoStream,
                            CreateNewStreamEventSequence(1, numberOfSeedEvents));
                    }

                    var allEventsPage = await eventStore.ReadAll(fromCheckpoint, maxCount, ReadDirection.Backward);

                    allEventsPage.StreamEvents.Length.ShouldBe(expectedCount);
                    allEventsPage.FromCheckpoint.ShouldBe(expectedFromCheckpoint);
                    allEventsPage.NextCheckpoint.ShouldBe(expectedNextCheckPoint);
                }
            }
        }
    }
}