namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class StreamSubscriptionTests
    {
        [Fact]
        public async Task When_exception_throw_by_subscription_event_received_then_should_drop_subscription()
        {
            using(var store = new InMemoryEventStore())
            {
                var eventReceivedException = new TaskCompletionSource<Exception>();
                StreamEventReceived eventReceived = _ =>
                {
                    throw new Exception();
                };
                SubscriptionDropped subscriptionDropped = (reason, exception) =>
                {
                    eventReceivedException.SetResult(exception);
                };
                string streamId = "stream-1";
                using(await store.SubscribeToStream("stream-1", StreamVersion.Start, eventReceived, subscriptionDropped))
                {
                    await store.AppendToStream(streamId,
                        ExpectedVersion.NoStream,
                        new NewStreamEvent(Guid.NewGuid(), "type", "{}"));

                    var dropException = await eventReceivedException.Task.WithTimeout();

                    dropException.ShouldBeOfType<Exception>();
                }
            }
        }
    }
}