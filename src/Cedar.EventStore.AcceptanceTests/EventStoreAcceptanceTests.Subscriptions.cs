namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_subscribe_to_a_stream_from_start()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    await AppendEvents(eventStore, streamId1, 10);

                    string streamId2 = "stream-2";
                    await AppendEvents(eventStore, streamId2, 10);

                    var done = new TaskCompletionSource<StreamEvent>();
                    var receivedEvents = new List<StreamEvent>();
                    using (var subscription = await eventStore.SubscribeToStream(
                        streamId1,
                        StreamVersion.Start,
                        streamEvent =>
                        {
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamVersion == 11)
                            {
                                done.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 2);

                        var receivedEvent = await done.Task.WithTimeout();

                        receivedEvents.Count.ShouldBe(12);
                        subscription.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamVersion.ShouldBe(11);
                        subscription.LastVersion.ShouldBeGreaterThan(0);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_a_stream_from_start_before_events_are_written()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId = "stream-1";

                    var done = new TaskCompletionSource<StreamEvent>();
                    var receivedEvents = new List<StreamEvent>();
                    using (var subscription = await eventStore.SubscribeToStream(
                        streamId,
                        StreamVersion.Start,
                        streamEvent =>
                        {
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamVersion == 1)
                            {
                                done.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId, 2);

                        var receivedEvent = await done.Task.WithTimeout();

                        receivedEvents.Count.ShouldBe(2);
                        subscription.StreamId.ShouldBe(streamId);
                        receivedEvent.StreamId.ShouldBe(streamId);
                        receivedEvent.StreamVersion.ShouldBe(1);
                        subscription.LastVersion.ShouldBeGreaterThan(0);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_all_stream_from_start()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    await AppendEvents(eventStore, streamId1, 3);

                    string streamId2 = "stream-2";
                    await AppendEvents(eventStore, streamId2, 3);

                    var receiveEvents = new TaskCompletionSource<StreamEvent>();
                    List<StreamEvent> receivedEvents = new List<StreamEvent>();
                    using(await eventStore.SubscribeToAll(
                        null,
                        streamEvent =>
                        {
                            _testOutputHelper.WriteLine($"Received event {streamEvent.StreamId} {streamEvent.StreamVersion} {streamEvent.Checkpoint}");
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamId == streamId1 && streamEvent.StreamVersion == 3)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 1);

                        await receiveEvents.Task.WithTimeout();

                        receivedEvents.Count.ShouldBe(7);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_all_stream_from_start_before_events_are_written()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";

                    string streamId2 = "stream-2";

                    var receiveEvents = new TaskCompletionSource<StreamEvent>();
                    List<StreamEvent> receivedEvents = new List<StreamEvent>();
                    using (await eventStore.SubscribeToAll(
                        null,
                        streamEvent =>
                        {
                            _testOutputHelper.WriteLine($"Received event {streamEvent.StreamId} {streamEvent.StreamVersion} {streamEvent.Checkpoint}");
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamId == streamId1 && streamEvent.StreamVersion == 3)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 3);

                        await AppendEvents(eventStore, streamId2, 3);

                        await AppendEvents(eventStore, streamId1, 1);

                        await receiveEvents.Task.WithTimeout();

                        receivedEvents.Count.ShouldBe(7);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_a_stream_from_end()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    await AppendEvents(eventStore, streamId1, 10);

                    string streamId2 = "stream-2";
                    await AppendEvents(eventStore, streamId2, 10);

                    var receiveEvents = new TaskCompletionSource<StreamEvent>();
                    int receivedCount = 0;
                    using (var subscription = await eventStore.SubscribeToStream(
                        streamId1,
                        StreamVersion.End,
                        streamEvent =>
                        {
                            _testOutputHelper.WriteLine($"Received event {streamEvent.StreamId} {streamEvent.StreamVersion} {streamEvent.Checkpoint}");
                            receivedCount++;
                            if (streamEvent.StreamVersion == 11)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 2);

                        var allEventsPage = await eventStore.ReadAllForwards(0, 30);
                        foreach(var streamEvent in allEventsPage.StreamEvents)
                        {
                            _testOutputHelper.WriteLine(streamEvent.ToString());
                        }

                        var receivedEvent = await receiveEvents.Task.WithTimeout();

                        receivedCount.ShouldBe(2);
                        subscription.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamVersion.ShouldBe(11);
                        subscription.LastVersion.ShouldBeGreaterThan(0);
                    }
                }
            }
        }

        [Fact]
        public async Task Given_non_empty_eventstore_can_subscribe_to_all_stream_from_end()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    await AppendEvents(eventStore, streamId1, 10);

                    string streamId2 = "stream-2";
                    await AppendEvents(eventStore, streamId2, 10);

                    var receiveEvents = new TaskCompletionSource<StreamEvent>();
                    List<StreamEvent> receivedEvents = new List<StreamEvent>();
                    using (await eventStore.SubscribeToAll(
                        Checkpoint.End,
                        streamEvent =>
                        {
                            _testOutputHelper.WriteLine($"StreamId={streamEvent.StreamId} Version={streamEvent.StreamVersion} ");
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamId == streamId1 && streamEvent.StreamVersion == 11)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 2);

                        await receiveEvents.Task.WithTimeout();

                        receivedEvents.Count.ShouldBe(2);
                    }
                }
            }
        }

        [Fact]
        public async Task Given_empty_eventstore_can_subscribe_to_all_stream_from_end()
        {
            var stopwatch = Stopwatch.StartNew();
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    var receiveEvents = new TaskCompletionSource<StreamEvent>();
                    List<StreamEvent> receivedEvents = new List<StreamEvent>();
                    using (await eventStore.SubscribeToAll(
                        Checkpoint.End,
                        streamEvent =>
                        {
                            _testOutputHelper.WriteLine($"{stopwatch.ElapsedMilliseconds.ToString()} {streamEvent.StreamVersion}");
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamId == streamId1 && streamEvent.StreamVersion == 9)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        
                        await AppendEvents(eventStore, streamId1, 10);

                        await receiveEvents.Task.WithTimeout();

                        receivedEvents.Count.ShouldBe(10);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_a_stream_from_a_specific_version()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    await AppendEvents(eventStore, streamId1, 10);

                    string streamId2 = "stream-2";
                    await AppendEvents(eventStore, streamId2, 10);

                    var receiveEvents = new TaskCompletionSource<StreamEvent>();
                    int receivedCount = 0;
                    using (var subscription = await eventStore.SubscribeToStream(
                        streamId1,
                        8,
                        streamEvent =>
                        {
                            receivedCount++;
                            if (streamEvent.StreamVersion == 11)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 2);

                        var receivedEvent = await receiveEvents.Task.WithTimeout();

                        receivedCount.ShouldBe(4);
                        subscription.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamVersion.ShouldBe(11);
                        subscription.LastVersion.ShouldBeGreaterThan(0);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_have_multiple_subscriptions_to_all()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    await AppendEvents(eventStore, streamId1, 2);

                    var subscriptionCount = fixture.LargeSubscriptionCount;

                    var completionSources =
                        Enumerable.Range(0, subscriptionCount).Select(_ => new TaskCompletionSource<int>())
                        .ToArray();

                    var subscriptions = await Task.WhenAll(Enumerable.Range(0, subscriptionCount)
                        .Select(async index => await eventStore.SubscribeToAll(
                            null,
                            streamEventReceived: streamEvent =>
                            {
                                if(streamEvent.StreamVersion == 1)
                                {
                                    completionSources[index].SetResult(0);
                                }
                                return Task.CompletedTask;
                            })));


                    try
                    {
                        await Task.WhenAll(completionSources.Select(source => source.Task)).WithTimeout();
                    }
                    finally
                    {
                        foreach (var subscription in subscriptions) subscription.Dispose();
                    }
                }
            }
        }

        [Fact]
        public async Task Can_have_multiple_subscriptions_to_stream()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    await AppendEvents(eventStore, streamId1, 2);

                    var subscriptionCount = fixture.LargeSubscriptionCount;

                    var completionSources =
                        Enumerable.Range(0, subscriptionCount).Select(_ => new TaskCompletionSource<int>())
                        .ToArray();

                    var subscriptions = await Task.WhenAll(Enumerable.Range(0, subscriptionCount)
                        .Select(async index => await eventStore.SubscribeToStream(
                            streamId1,
                            0,
                            streamEventReceived: streamEvent =>
                            {
                                if (streamEvent.StreamVersion == 1)
                                {
                                    completionSources[index].SetResult(0);
                                }
                                return Task.CompletedTask;
                            })));


                    try
                    {
                        await Task.WhenAll(completionSources.Select(source => source.Task)).WithTimeout();
                    }
                    finally
                    {
                        foreach (var subscription in subscriptions) subscription.Dispose();
                    }
                }
            }
        }

        private static async Task AppendEvents(IEventStore eventStore, string streamId, int numberOfEvents)
        {
            for(int i = 0; i < numberOfEvents; i++)
            {
                var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEvent", "{}");
                await eventStore.AppendToStream(streamId, ExpectedVersion.Any, newStreamEvent);
            }
        }
    }
}