namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
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

                    var receiveEvents = new TaskCompletionSource<StreamEvent>();
                    int receivedCount = 0;
                    using (var subscription = await eventStore.SubscribeToStream(
                        streamId1,
                        StreamVersion.Start,
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

                        var receivedEvent = await receiveEvents.Task.WithTimeout(1000);

                        receivedCount.ShouldBe(12);
                        subscription.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamVersion.ShouldBe(11);
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
                    using(var subscription = await eventStore.SubscribeToAll(
                        eventStore.StartCheckpoint,
                        streamEvent =>
                        {
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamId == streamId1 && streamEvent.StreamVersion == 3)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 1);

                        await receiveEvents.Task.WithTimeout(1000);

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
                            receivedCount++;
                            if (streamEvent.StreamVersion == 11)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 2);

                        var receivedEvent = await receiveEvents.Task.WithTimeout(5000);

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
                        eventStore.EndCheckpoint,
                        streamEvent =>
                        {
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamId == streamId1 && streamEvent.StreamVersion == 11)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 2);

                        await receiveEvents.Task.WithTimeout(1000);

                        receivedEvents.Count.ShouldBe(2);
                    }
                }
            }
        }

        [Fact]
        public async Task Given_empty_eventstore_can_subscribe_to_all_stream_from_end()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    string streamId1 = "stream-1";
                    var receiveEvents = new TaskCompletionSource<StreamEvent>();
                    List<StreamEvent> receivedEvents = new List<StreamEvent>();
                    using (await eventStore.SubscribeToAll(
                        eventStore.EndCheckpoint,
                        streamEvent =>
                        {
                            receivedEvents.Add(streamEvent);
                            if (streamEvent.StreamId == streamId1 && streamEvent.StreamVersion == 9)
                            {
                                receiveEvents.SetResult(streamEvent);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        
                        await AppendEvents(eventStore, streamId1, 10);

                        await receiveEvents.Task.WithTimeout(5000);

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
                        },
                        (reason, exception) =>
                        {
                            if (exception != null)
                            {
                                receiveEvents.SetException(exception);
                            }
                        }))
                    {
                        await AppendEvents(eventStore, streamId1, 2);

                        var receivedEvent = await receiveEvents.Task;

                        receivedCount.ShouldBe(4);
                        subscription.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamId.ShouldBe(streamId1);
                        receivedEvent.StreamVersion.ShouldBe(11);
                        subscription.LastVersion.ShouldBeGreaterThan(0);
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