namespace Cedar.EventStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task When_delete_event_then_event_should_not_be_in_stream()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore.DeleteEvent(streamId, 1);

                    var streamEventsPage = await eventStore.ReadStreamForwards(streamId, StreamVersion.Start, 3, CancellationToken.None);

                    streamEventsPage.Events.Length.ShouldBe(2);
                }
            }
        }
    }
}
