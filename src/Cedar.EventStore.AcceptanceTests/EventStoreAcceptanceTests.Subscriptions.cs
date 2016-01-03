namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_subscribe_to_a_stream()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetEventStore())
                {
                    string streamId = "stream-1";
                    var receiveEvent = new TaskCompletionSource<StreamEvent>();
                    await store.SubscribeToStream(
                        streamId,
                        streamEvent =>
                        {
                            receiveEvent.SetResult(streamEvent);
                            return Task.CompletedTask;
                        },
                        (reason, exception) => receiveEvent.SetException(exception));
                    var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEvent", "{}");
                    await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);

                    await Task.Delay(2000);
                    newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEvent", "{}");
                    await store.AppendToStream(streamId, ExpectedVersion.Any, newStreamEvent);

                    var receivedEvent = await receiveEvent.Task;

                    receivedEvent.StreamId.ShouldBe(streamId);
                }
            }
        }
    }
}