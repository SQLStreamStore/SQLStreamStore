namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class AcceptanceTests
    {
        [Fact]
        public async Task Given_empty_store_when_get_head_position_Then_should_be_minus_one()
        {
            var head = await Store.ReadHeadPosition();

            head.ShouldBe(-1);
        }

        [Fact]
        public async Task Given_store_with_messages_then_can_get_head_position()
        {
            await Store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

            var head = await Store.ReadHeadPosition();

            head.ShouldBe(2);
        }
    }
}
