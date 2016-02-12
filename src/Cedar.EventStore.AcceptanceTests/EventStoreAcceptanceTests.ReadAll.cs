namespace Cedar.EventStore
{
    using System;
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
        public async Task When_read_all_forwards_from_an_empty_store_than_should_have_checkpoints()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    var allEventsPage = await eventStore.ReadAll(Checkpoint.Start, 4);

                    allEventsPage.FromCheckpoint.ShouldBe(Checkpoint.Start);
                    allEventsPage.NextCheckpoint.ShouldBe(Checkpoint.Start);
                }
            }
        }

        [Fact]
        public async Task When_read_all_backwards_from_an_empty_store_than_should_have_checkpoints()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    var allEventsPage = await eventStore.ReadAll(Checkpoint.End, 5, ReadDirection.Backward);

                    allEventsPage.FromCheckpoint.ShouldBe(Checkpoint.End);
                    allEventsPage.NextCheckpoint.ShouldBe(Checkpoint.Start);
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

                    int count = 0; //counter is used to short circuit bad implementations that never return IsEnd = true

                    while (!allEventsPage.IsEnd && count < 20)
                    {
                        allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, 4);
                        count++;
                    }

                    allEventsPage.IsEnd.ShouldBeTrue();

                    var currentCheckpoint = allEventsPage.NextCheckpoint;

                    // read end of stream again, should be empty, should return same checkpoint
                    allEventsPage = await eventStore.ReadAll(currentCheckpoint, 10);
                    count = 0;
                    while (!allEventsPage.IsEnd && count < 20)
                    {
                        allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, 10);
                        count++;
                    }

                    allEventsPage.IsEnd.ShouldBeTrue();
                    allEventsPage.NextCheckpoint.ShouldNotBeNull();

                    currentCheckpoint = allEventsPage.NextCheckpoint;

                    // append some events then read again from the saved checkpoint, the next checkpoint should have moved
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.Any, CreateNewStreamEvents(7, 8, 9));

                    allEventsPage = await eventStore.ReadAll(currentCheckpoint, 10);
                    count = 0;
                    while (!allEventsPage.IsEnd && count < 20)
                    {
                        allEventsPage = await eventStore.ReadAll(allEventsPage.NextCheckpoint, 10);
                        count++;
                    }

                    allEventsPage.IsEnd.ShouldBeTrue();
                    allEventsPage.NextCheckpoint.ShouldNotBeNull();
                }
            }
        }

        [Fact]
        public async Task When_read_past_end_of_all()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    bool isEnd = false;
                    int count = 0;
                    var checkpoint = Checkpoint.Start;
                    while (!isEnd)
                    {
                        _testOutputHelper.WriteLine($"Loop {count}");

                        var streamEventsPage = await eventStore.ReadAll(checkpoint, 10);
                        _testOutputHelper.WriteLine($"FromCheckpoint     = {streamEventsPage.FromCheckpoint}");
                        _testOutputHelper.WriteLine($"NextCheckpoint     = {streamEventsPage.NextCheckpoint}");
                        _testOutputHelper.WriteLine($"IsEnd              = {streamEventsPage.IsEnd}");
                        _testOutputHelper.WriteLine($"StreamEvents.Count = {streamEventsPage.StreamEvents.Count}");
                        _testOutputHelper.WriteLine("");

                        checkpoint = streamEventsPage.NextCheckpoint;
                        isEnd = streamEventsPage.IsEnd;
                        count++;

                        if(count > 100)
                        {
                            throw new Exception("omg wtf");
                        }
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
    }
}