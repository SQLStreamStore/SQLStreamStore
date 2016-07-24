namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task Given_empty_store_when_get_head_checkpoint_Then_should_be_minus_one()
        {
            using(var fixture = GetFixture())
            {
                var store = await fixture.GetStreamStore();

                var head = await store.ReadHeadCheckpoint();

                head.ShouldBe(-1);
            }
        }

        [Fact]
        public async Task Given_store_with_events_then_can_get_head_checkpoint()
        {
            using (var fixture = GetFixture())
            {
                var eventStore = await fixture.GetStreamStore();

                await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                var head = await eventStore.ReadHeadCheckpoint();

                head.ShouldBe(2);
            }
        }
    }
}
