﻿namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Nito.AsyncEx;
    using Shouldly;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;
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
                    using (var subscription = store.SubscribeToStream(
                        streamId1,
                        null,
                        (_, message) =>
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
                        subscription.LastVersion.Value.ShouldBeGreaterThan(0);
                    }
                }
            }
        }

        [Fact]
        public async Task When_subscribe_to_a_stream_and_receive_message_then_should_get_subscription_instance()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await AppendMessages(store, streamId, 10);

                    var done = new TaskCompletionSource<IStreamSubscription>();
                    using (var subscription = store.SubscribeToStream(
                        streamId,
                        null,
                        (sub, _) =>
                        {
                            done.SetResult(sub);
                            return Task.CompletedTask;
                        }))
                    {
                        var receivedSubscription = await done.Task.WithTimeout();

                        receivedSubscription.ShouldBe(subscription);
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
                    using (var subscription = store.SubscribeToStream(
                        streamId,
                        null,
                        (_, message) =>
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
                        subscription.LastVersion.Value.ShouldBeGreaterThan(0);
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
                    using(store.SubscribeToAll(
                        null,
                        (_, message) =>
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
        public async Task When_subscribe_to_all_and_receive_message_then_should_get_subscription_instance()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await AppendMessages(store, streamId, 10);

                    var done = new TaskCompletionSource<IAllStreamSubscription>();
                    using (var subscription = store.SubscribeToAll(
                        null,
                        (sub, _) =>
                        {
                            done.SetResult(sub);
                            return Task.CompletedTask;
                        }))
                    {
                        var receivedSubscription = await done.Task.WithTimeout();

                        receivedSubscription.ShouldBe(subscription);
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
                    using (store.SubscribeToAll(
                        null,
                        (_, message) =>
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
                    using (var subscription = store.SubscribeToStream(
                        streamId1,
                        StreamVersion.End,
                        (_, message) =>
                        {
                            _testOutputHelper.WriteLine($"Received message {message.StreamId} {message.StreamVersion} "
                                                        + $"{message.Position}");
                            receivedCount++;
                            if (message.StreamVersion >= 11)
                            {
                                receiveMessage.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await subscription.Started;
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
                        subscription.LastVersion.Value.ShouldBeGreaterThan(0);
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
                    using(var subscription = store.SubscribeToAll(
                        Position.End,
                        (_, message) =>
                        {
                            _testOutputHelper.WriteLine($"StreamId={message.StreamId} Version={message.StreamVersion} "
                                                        + $"Position={message.Position}");
                            receivedMessages.Add(message);
                            if (message.StreamId == streamId1 && message.StreamVersion == 11)
                            {
                                receiveMessages.SetResult(message);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await subscription.Started;

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
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId1 = "stream-1";
                    var receiveMessages = new TaskCompletionSource<int>();
                    List<StreamMessage> receivedMessages = new List<StreamMessage>();
                    using (var subscription = store.SubscribeToAll(
                        Position.End,
                        (_, message) =>
                        {
                            receivedMessages.Add(message);
                            if (message.StreamId == streamId1 && message.StreamVersion == 9)
                            {
                                receiveMessages.SetResult(0);
                            }
                            return Task.CompletedTask;
                        }))
                    {
                        await subscription.Started;

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
                    using (var subscription = store.SubscribeToStream(
                        streamId1,
                        7,
                        (_, message) =>
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
                        subscription.LastVersion.Value.ShouldBeGreaterThan(0);
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

                    var subscriptions = Enumerable.Range(0, subscriptionCount)
                        .Select(index => store.SubscribeToAll(
                            null,
                            (_, message) =>
                            {
                                if(message.StreamVersion == 1)
                                {
                                    completionSources[index].SetResult(0);
                                }
                                return Task.CompletedTask;
                            }))
                        .ToArray();


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

                    var subscriptionCount = 50;

                    var completionSources =
                        Enumerable.Range(0, subscriptionCount)
                        .Select(_ => new TaskCompletionSource<int>())
                        .ToArray();

                    var subscriptions = Enumerable.Range(0, subscriptionCount)
                        .Select(index => store.SubscribeToStream(
                            streamId1,
                            0,
                            (_, message) =>
                            {
                                if(message.StreamVersion == 1)
                                {
                                    completionSources[index].SetResult(0);
                                }
                                return Task.CompletedTask;
                            }))
                        .ToArray();

                    try
                    {
                        await Task.WhenAll(completionSources.Select(source => source.Task)).WithTimeout();
                    }
                    finally
                    {
                        foreach(var subscription in subscriptions)
                        {
                            subscription.Dispose();
                        }
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
                    using (store.SubscribeToAll(
                        null,
                        (_, message) =>
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

        [Fact]
        public async Task When_exception_throw_by_stream_subscriber_then_should_drop_subscription_with_reason_SubscriberError()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    var eventReceivedException = new TaskCompletionSource<SubscriptionDroppedReason>();
                    StreamMessageReceived messageReceived = (_, __) =>
                    {
                        throw new Exception();
                    };
                    SubscriptionDropped subscriptionDropped = (_, reason, __) =>
                    {
                        eventReceivedException.SetResult(reason);
                    };
                    string streamId = "stream-1";
                    using(store.SubscribeToStream("stream-1",
                        null,
                        messageReceived,
                        subscriptionDropped))
                    {
                        await store.AppendToStream(streamId,
                            ExpectedVersion.NoStream,
                            new NewStreamMessage(Guid.NewGuid(), "type", "{}"));

                        var droppedReason = await eventReceivedException.Task.WithTimeout();

                        droppedReason.ShouldBe(SubscriptionDroppedReason.SubscriberError);
                    }
                }
            }
        }

        [Fact]
        public async Task When_stream_subscription_disposed_then_should_drop_subscription_with_reason_Disposed()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    var tcs = new TaskCompletionSource<SubscriptionDroppedReason>();
                    var subscription = store.SubscribeToStream("stream-1",
                        StreamVersion.End,
                        (_, __) => Task.CompletedTask,
                        (_, reason, __) =>
                        {
                            tcs.SetResult(reason);
                        });
                    subscription.Dispose();
                    var droppedReason = await tcs.Task.WithTimeout();

                    droppedReason.ShouldBe(SubscriptionDroppedReason.Disposed);
                }
            }
        }

        [Fact]
        public async Task When_stream_subscription_dropped_then_should_supply_subscription_instance()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    var tcs = new TaskCompletionSource<IStreamSubscription>();
                    var subscription = store.SubscribeToStream(
                        "stream-1",
                        null,
                        (_, __) => Task.CompletedTask,
                        (sub, _, __) =>
                        {
                            tcs.SetResult(sub);
                        });

                    subscription.Dispose();
                    var receivedSubscriptiuon = await tcs.Task.WithTimeout();

                    receivedSubscriptiuon.ShouldBe(subscription);
                }
            }
        }

        [Fact]
        public async Task When_stream_subscription_disposed_while_handling_messages_then_should_drop_subscription_with_reason_Disposed()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    var droppedTcs = new TaskCompletionSource<SubscriptionDroppedReason>();
                    var handler = new AsyncAutoResetEvent();
                    var subscription = store.SubscribeToStream(streamId,
                        StreamVersion.Start,
                        async (_, __) =>
                        {
                            handler.Set();
                            await handler.WaitAsync(); // block "handling" while a dispose occurs
                        },
                        (_, reason, __) =>
                        {
                            droppedTcs.SetResult(reason);
                        });
                    // First message is blocked in handling, the second is co-operatively cancelled
                    await AppendMessages(store, streamId, 2); 
                    await handler.WaitAsync();
                    subscription.Dispose();
                    handler.Set();

                    var droppedReason = await droppedTcs.Task.WithTimeout();

                    droppedReason.ShouldBe(SubscriptionDroppedReason.Disposed);
                }
            }
        }

        [Fact]
        public async Task Can_dispose_stream_subscription_multiple_times()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    var subscription = store.SubscribeToStream(streamId,
                        StreamVersion.Start,
                        (_, __) => Task.CompletedTask);
                    await AppendMessages(store, streamId, 2);
                    subscription.Dispose();
                    subscription.Dispose();
                }
            }
        }

        [Fact]
        public async Task When_exception_throw_by_all_stream_subscriber_then_should_drop_subscription_with_reason_SubscriberError()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    var eventReceivedException = new TaskCompletionSource<SubscriptionDroppedReason>();
                    AllStreamMessageReceived messageReceived = (_, __) =>
                    {
                        throw new Exception();
                    };
                    AllSubscriptionDropped subscriptionDropped = (_, reason, __) =>
                    {
                        eventReceivedException.SetResult(reason);
                    };
                    string streamId = "stream-1";
                    using (store.SubscribeToAll(
                        null,
                        messageReceived,
                        subscriptionDropped))
                    {
                        await store.AppendToStream(streamId,
                            ExpectedVersion.NoStream,
                            new NewStreamMessage(Guid.NewGuid(), "type", "{}"));

                        var droppedReason = await eventReceivedException.Task.WithTimeout();

                        droppedReason.ShouldBe(SubscriptionDroppedReason.SubscriberError);
                    }
                }
            }
        }

        [Fact]
        public async Task When_all_stream_subscription_disposed_then_should_drop_subscription_with_reason_Disposed()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    var tcs = new TaskCompletionSource<SubscriptionDroppedReason>();
                    var subscription = store.SubscribeToAll(
                        Position.End,
                        (_, __) => Task.CompletedTask,
                        (_, reason, __) =>
                        {
                            tcs.SetResult(reason);
                        });
                    subscription.Dispose();
                    var droppedReason = await tcs.Task.WithTimeout();

                    droppedReason.ShouldBe(SubscriptionDroppedReason.Disposed);
                }
            }
        }

        [Fact]
        public async Task When_all_stream_subscription_dropped_then_should_supply_subscription_instance()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    var tcs = new TaskCompletionSource<IAllStreamSubscription>();
                    var subscription = store.SubscribeToAll(
                        Position.End,
                        (_, __) => Task.CompletedTask,
                        (sub, _, __) =>
                        {
                            tcs.SetResult(sub);
                        });
                    subscription.Dispose();
                    var receivedSubscriptiuon = await tcs.Task.WithTimeout();

                    receivedSubscriptiuon.ShouldBe(subscription);
                }
            }
        }

        [Fact]
        public async Task When_all_stream_subscription_disposed_while_handling_messages_then_should_drop_subscription_with_reason_Disposed()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    var droppedTcs = new TaskCompletionSource<SubscriptionDroppedReason>();
                    var handler = new AsyncAutoResetEvent();
                    var subscription = store.SubscribeToAll(
                        Position.End,
                        async (_, __) =>
                        {
                            handler.Set();
                            await handler.WaitAsync().WithTimeout(); // block "handling" while a dispose occurs
                        },
                        (_, reason, __) =>
                        {
                            droppedTcs.SetResult(reason);
                        });
                    // First message is blocked in handling, the second is co-operatively cancelled
                    await subscription.Started;
                    await AppendMessages(store, streamId, 2);
                    await handler.WaitAsync().WithTimeout(5000);
                    subscription.Dispose();
                    handler.Set();

                    var droppedReason = await droppedTcs.Task.WithTimeout();

                    droppedReason.ShouldBe(SubscriptionDroppedReason.Disposed);
                }
            }
        }

        [Fact]
        public async Task Can_dispose_all_stream_subscription_multiple_times()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    var subscription = store.SubscribeToAll(
                        Position.Start,
                        (_, __) => Task.CompletedTask);
                    await AppendMessages(store, streamId, 2);
                    subscription.Dispose();
                    subscription.Dispose();
                }
            }
        }

        [Fact]
        public async Task When_caughtup_to_all_then_then_should_notify()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    await AppendMessages(store, streamId, 30);
                    var caughtUp = new TaskCompletionSource<bool>();
                    var subscription = store.SubscribeToAll(
                        null,
                        (_, __) => Task.CompletedTask,
                        hasCaughtUp: b =>
                        {
                            if(b)
                            {
                                caughtUp.SetResult(b);
                            }
                        });
                    subscription.MaxCountPerRead = 10;
                    await caughtUp.Task.WithTimeout(5000);
                    subscription.Dispose();
                }
            }
        }

        [Fact]
        public async Task When_caughtup_to_stream_then_then_should_notify()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    await AppendMessages(store, streamId, 30);
                    var caughtUp = new TaskCompletionSource<bool>();
                    var subscription = store.SubscribeToStream(
                        streamId,
                        null,
                        (_, __) => Task.CompletedTask,
                        hasCaughtUp: b =>
                        {
                            if (b)
                            {
                                caughtUp.SetResult(b);
                            }
                        });
                    subscription.MaxCountPerRead = 10;
                    await caughtUp.Task.WithTimeout(5000);
                    subscription.Dispose();
                }
            }
        }

        [Fact]
        public async Task When_falls_behind_on_all_then_then_should_notify()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    await AppendMessages(store, streamId, 30);
                    var fallenBehind = new TaskCompletionSource<bool>();
                    bool caughtUp = false;

                    var subscription = store.SubscribeToAll(
                        null,
                        (_, __) => Task.CompletedTask,
                        hasCaughtUp: b =>
                        {
                            if (b)
                            {
                                caughtUp = true;
                            }
                            if (b && caughtUp)
                            {
                                fallenBehind.SetResult(b);
                            }
                        });
                    subscription.MaxCountPerRead = 10;

                    await fallenBehind.Task.WithTimeout(5000);
                    subscription.Dispose();
                }
            }
        }

        [Fact]
        public async Task When_falls_behind_on_stream_then_then_should_notify()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    await AppendMessages(store, streamId, 30);
                    var fallenBehind = new TaskCompletionSource<bool>();
                    bool caughtUp = false;

                    var subscription = store.SubscribeToStream(
                        streamId,
                        null,
                        (_, __) => Task.CompletedTask,
                        hasCaughtUp: b =>
                        {
                            if (b)
                            {
                                caughtUp = true;
                            }
                            if(b && caughtUp)
                            {
                                fallenBehind.SetResult(b);
                            }
                        });
                    subscription.MaxCountPerRead = 10;

                    await fallenBehind.Task.WithTimeout(5000);
                    subscription.Dispose();
                }
            }
        }

        [Fact]
        public async Task When_dispose_store_then_should_dispose_stream_subscriptions()
        {
            using (var fixture = GetFixture())
            {
                var store = await fixture.GetStreamStore();
                var subscriptionDropped = new TaskCompletionSource<SubscriptionDroppedReason>();
                var subscription = store.SubscribeToStream(
                    "stream-1",
                    null,
                    (_, __) => Task.CompletedTask,
                    subscriptionDropped: (streamSubscription, reason, exception) =>
                    {
                        subscriptionDropped.SetResult(reason);
                    });

                store.Dispose();

                var droppedReason = await subscriptionDropped.Task.WithTimeout(5000);

                droppedReason.ShouldBe(SubscriptionDroppedReason.Disposed);
            }
        }

        [Fact]
        public async Task When_dispose_store_then_should_dispose_all_stream_subscriptions()
        {
            using (var fixture = GetFixture())
            {
                var store = await fixture.GetStreamStore();
                var subscriptionDropped = new TaskCompletionSource<SubscriptionDroppedReason>();
                var subscription = store.SubscribeToAll(
                    null,
                    (_, __) => Task.CompletedTask,
                    subscriptionDropped: (streamSubscription, reason, exception) =>
                    {
                        subscriptionDropped.SetResult(reason);
                    });

                store.Dispose();

                var droppedReason = await subscriptionDropped.Task.WithTimeout(5000);

                droppedReason.ShouldBe(SubscriptionDroppedReason.Disposed);
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
