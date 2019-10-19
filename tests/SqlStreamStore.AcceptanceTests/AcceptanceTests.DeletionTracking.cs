namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using static Streams.Deleted;

    partial class AcceptanceTests
    {
        [Fact]
        public async Task When_deletion_tracking_is_disabled_deleted_message_should_not_be_tracked()
        {
            Fixture.DisableDeletionTracking = true;

            var messages = CreateNewStreamMessages(1);

            await Fixture.Store.AppendToStream("stream", ExpectedVersion.NoStream, messages);

            await Fixture.Store.DeleteMessage("stream", messages[0].MessageId);

            var page = await Store.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);

            page.Messages.Length.ShouldBe(0);
        }

        [Fact]
        public async Task When_deletion_tracking_is_disabled_deleted_stream_should_not_be_tracked()
        {
            Fixture.DisableDeletionTracking = true;
            
            var messages = CreateNewStreamMessages(1);

            await Fixture.Store.AppendToStream("stream", ExpectedVersion.NoStream, messages);

            await Fixture.Store.DeleteStream("stream");

            var page = await Store.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);

            page.Messages.Length.ShouldBe(0);
        }
    }
}