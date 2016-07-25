namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_subscribe_to_a_stream_from_start()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    await AppendMessages(store, streamId1, 10);

                    string streamId2 = "stream-2";
                    await AppendMessages(store, streamId2, 10);

                    var done = new TaskCompletionSource<StreamMessage>();
                    var receivedMessages = new List<StreamMessage>();
                    using (var subscription = await store.SubscribeToStream(
                        streamId1,
                        StreamVersion.Start,
                        message =>
                        {
                            receivedMessages.Add(message);
                            if (message.StreamVersion == 11)
                            {
                                done.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendMessages(store, streamId1, 2);

                        var receivedMessage = await done.Task.WithTimeout();

                        receivedMessages.Count.ShouldBe(12);
                        subscription.StreamId.ShouldBe(streamId1);
                        receivedMessage.StreamId.ShouldBe(streamId1);
                        receivedMessage.StreamVersion.ShouldBe(11);
                        subscription.LastVersion.ShouldBeGreaterThan(0);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_a_stream_from_start_before_messages_are_written()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";

                    var done = new TaskCompletionSource<StreamMessage>();
                    var receivedMessages = new List<StreamMessage>();
                    using (var subscription = await store.SubscribeToStream(
                        streamId,
                        StreamVersion.Start,
                        message =>
                        {
                            receivedMessages.Add(message);
                            if (message.StreamVersion == 1)
                            {
                                done.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendMessages(store, streamId, 2);

                        var receivedMessage = await done.Task.WithTimeout();

                        receivedMessages.Count.ShouldBe(2);
                        subscription.StreamId.ShouldBe(streamId);
                        receivedMessage.StreamId.ShouldBe(streamId);
                        receivedMessage.StreamVersion.ShouldBe(1);
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
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    await AppendMessages(store, streamId1, 3);

                    string streamId2 = "stream-2";
                    await AppendMessages(store, streamId2, 3);

                    var receiveMessages = new TaskCompletionSource<StreamMessage>();
                    List<StreamMessage> receivedMessages = new List<StreamMessage>();
                    using(await store.SubscribeToAll(
                        null,
                        message =>
                        {
                            _testOutputHelper.WriteLine($"Received message {message.StreamId} " +
                                                        $"{message.StreamVersion} {message.Position}");
                            receivedMessages.Add(message);
                            if (message.StreamId == streamId1 && message.StreamVersion == 3)
                            {
                                receiveMessages.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendMessages(store, streamId1, 1);

                        await receiveMessages.Task.WithTimeout();

                        receivedMessages.Count.ShouldBe(7);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_all_stream_from_start_before_messages_are_written()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";

                    string streamId2 = "stream-2";

                    var receiveMessages = new TaskCompletionSource<StreamMessage>();
                    List<StreamMessage> receivedMessages = new List<StreamMessage>();
                    using (await store.SubscribeToAll(
                        null,
                        message =>
                        {
                            _testOutputHelper.WriteLine($"Received message {message.StreamId} {message.StreamVersion} {message.Position}");
                            receivedMessages.Add(message);
                            if (message.StreamId == streamId1 && message.StreamVersion == 3)
                            {
                                receiveMessages.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendMessages(store, streamId1, 3);

                        await AppendMessages(store, streamId2, 3);

                        await AppendMessages(store, streamId1, 1);

                        await receiveMessages.Task.WithTimeout();

                        receivedMessages.Count.ShouldBe(7);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_a_stream_from_end()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    await AppendMessages(store, streamId1, 10);

                    string streamId2 = "stream-2";
                    await AppendMessages(store, streamId2, 10);

                    var receiveMessage = new TaskCompletionSource<StreamMessage>();
                    int receivedCount = 0;
                    using (var subscription = await store.SubscribeToStream(
                        streamId1,
                        StreamVersion.End,
                        message =>
                        {
                            _testOutputHelper.WriteLine($"Received message {message.StreamId} {message.StreamVersion} {message.Position}");
                            receivedCount++;
                            if (message.StreamVersion == 11)
                            {
                                receiveMessage.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendMessages(store, streamId1, 2);

                        var allMessagesPage = await store.ReadAllForwards(0, 30);
                        foreach(var streamMessage in allMessagesPage.Messages)
                        {
                            _testOutputHelper.WriteLine(streamMessage.ToString());
                        }

                        var receivedMessage = await receiveMessage.Task.WithTimeout();

                        receivedCount.ShouldBe(2);
                        subscription.StreamId.ShouldBe(streamId1);
                        receivedMessage.StreamId.ShouldBe(streamId1);
                        receivedMessage.StreamVersion.ShouldBe(11);
                        subscription.LastVersion.ShouldBeGreaterThan(0);
                    }
                }
            }
        }

        [Fact]
        public async Task Given_non_empty_streamstore_can_subscribe_to_all_stream_from_end()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    await AppendMessages(store, streamId1, 10);

                    string streamId2 = "stream-2";
                    await AppendMessages(store, streamId2, 10);

                    var receiveMessages = new TaskCompletionSource<StreamMessage>();
                    List<StreamMessage> receivedMessages = new List<StreamMessage>();
                    using (await store.SubscribeToAll(
                        Position.End,
                        message =>
                        {
                            _testOutputHelper.WriteLine($"StreamId={message.StreamId} Version={message.StreamVersion} ");
                            receivedMessages.Add(message);
                            if (message.StreamId == streamId1 && message.StreamVersion == 11)
                            {
                                receiveMessages.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendMessages(store, streamId1, 2);

                        await receiveMessages.Task.WithTimeout();

                        receivedMessages.Count.ShouldBe(2);
                    }
                }
            }
        }

        [Fact]
        public async Task Given_empty_streamstore_can_subscribe_to_all_stream_from_end()
        {
            var stopwatch = Stopwatch.StartNew();
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    var receiveMessages = new TaskCompletionSource<StreamMessage>();
                    List<StreamMessage> receivedMessages = new List<StreamMessage>();
                    using (await store.SubscribeToAll(
                        Position.End,
                        message =>
                        {
                            _testOutputHelper.WriteLine($"{stopwatch.ElapsedMilliseconds.ToString()} {message.StreamVersion}");
                            receivedMessages.Add(message);
                            if (message.StreamId == streamId1 && message.StreamVersion == 9)
                            {
                                receiveMessages.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {

                        await AppendMessages(store, streamId1, 10);

                        await receiveMessages.Task.WithTimeout();

                        receivedMessages.Count.ShouldBe(10);
                    }
                }
            }
        }

        [Fact]
        public async Task Can_subscribe_to_a_stream_from_a_specific_version()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    await AppendMessages(store, streamId1, 10);

                    string streamId2 = "stream-2";
                    await AppendMessages(store, streamId2, 10);

                    var receiveMessages = new TaskCompletionSource<StreamMessage>();
                    int receivedCount = 0;
                    using (var subscription = await store.SubscribeToStream(
                        streamId1,
                        8,
                        message =>
                        {
                            receivedCount++;
                            if (message.StreamVersion == 11)
                            {
                                receiveMessages.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendMessages(store, streamId1, 2);

                        var receivedMessage = await receiveMessages.Task.WithTimeout();

                        receivedCount.ShouldBe(4);
                        subscription.StreamId.ShouldBe(streamId1);
                        receivedMessage.StreamId.ShouldBe(streamId1);
                        receivedMessage.StreamVersion.ShouldBe(11);
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
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    await AppendMessages(store, streamId1, 2);

                    var subscriptionCount = 500;

                    var completionSources =
                        Enumerable.Range(0, subscriptionCount).Select(_ => new TaskCompletionSource<int>())
                        .ToArray();

                    var subscriptions = await Task.WhenAll(Enumerable.Range(0, subscriptionCount)
                        .Select(async index => await store.SubscribeToAll(
                            null,
                            streamMessageReceived: message =>
                            {
                                if(message.StreamVersion == 1)
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
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    await AppendMessages(store, streamId1, 2);

                    var subscriptionCount = 500;

                    var completionSources =
                        Enumerable.Range(0, subscriptionCount).Select(_ => new TaskCompletionSource<int>())
                        .ToArray();

                    var subscriptions = await Task.WhenAll(Enumerable.Range(0, subscriptionCount)
                        .Select(async index => await store.SubscribeToStream(
                            streamId1,
                            0,
                            streamMessageReceived: message =>
                            {
                                if (message.StreamVersion == 1)
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
        public async Task When_delete_then_deleted_message_should_have_correct_position()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    // Arrange
                    string streamId1 = "stream-1";

                    var receiveMessage = new TaskCompletionSource<StreamMessage>();
                    List<StreamMessage> receivedMessages = new List<StreamMessage>();
                    using (await store.SubscribeToAll(
                        null,
                        message =>
                        {
                            _testOutputHelper.WriteLine($"Received message {message.StreamId} " +
                                                        $"{message.StreamVersion} {message.Position}");
                            receivedMessages.Add(message);
                            if (message.StreamId == Deleted.DeletedStreamId
                                && message.Type == Deleted.StreamDeletedMessageType)
                            {
                                receiveMessage.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await AppendMessages(store, streamId1, 1);

                        // Act
                        await store.DeleteStream(streamId1);
                        await receiveMessage.Task.WithTimeout();

                        // Assert
                        receivedMessages.Last().Position.ShouldBe(1);
                    }
                }
            }
        }

        private static async Task AppendMessages(IStreamStore streamStore, string streamId, int numberOfEvents)
        {
            for(int i = 0; i < numberOfEvents; i++)
            {
                var newmessage = new NewStreamMessage(Guid.NewGuid(), "MyEvent", "{}");
                await streamStore.AppendToStream(streamId, ExpectedVersion.Any, newmessage);
            }
        }
    }
}
