namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class ReadHeadCheckpointsAcceptanceTests : AcceptanceTests
    {
        public ReadHeadCheckpointsAcceptanceTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        { }

        [Fact]
        public async Task Given_empty_store_when_get_head_position_Then_should_be_minus_one()
        {
            var head = await store.ReadHeadPosition();

            head.ShouldBe(-1);
        }

        [Fact]
        public async Task Given_store_with_messages_then_can_get_head_position()
        {
            await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

            var head = await store.ReadHeadPosition();

            head.ShouldBe(2);
        }
    }
}
