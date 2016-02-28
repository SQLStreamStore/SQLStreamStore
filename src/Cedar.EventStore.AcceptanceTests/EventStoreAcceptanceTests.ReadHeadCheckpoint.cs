namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task Given_empty_store_when_get_head_checkpoint_Then_should_be_minus_one()
        {
            using(var fixture = GetFixture())
            {
                var store = await fixture.GetEventStore();

                var head = await store.ReadHeadCheckpoint();

                head.ShouldBe(-1);
            }
        }

        [Fact]
        public async Task Given_store_with_events_then_can_get_head_checkpoint()
        {
            using (var fixture = GetFixture())
            {
                var eventStore = await fixture.GetEventStore();

                await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                var head = await eventStore.ReadHeadCheckpoint();

                head.ShouldBe(2);
            }
        }
    }
}