namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using static Streams.Deleted;

    public partial class AcceptanceTests
    {
        [Fact, Trait("Category", "DeleteEvent")]
        public async Task When_delete_message_then_message_should_be_removed_from_stream()
        {
            const string streamId = "stream";
            var newStreamMessages = CreateNewStreamMessages(1, 2, 3);
            await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamMessages);
            var idToDelete = newStreamMessages[1].MessageId;

            await store.DeleteMessage(streamId, idToDelete);

            var streamMessagesPage = await store.ReadStreamForwards(streamId, StreamVersion.Start, 3);

            streamMessagesPage.Messages.Length.ShouldBe(2);
            streamMessagesPage.Messages.Any(e => e.MessageId == idToDelete).ShouldBeFalse();
        }

        [Fact, Trait("Category", "DeleteEvent")]
        public async Task When_delete_message_then_deleted_message_should_be_appended_to_deleted_stream()
        {
            const string streamId = "stream";
            var newStreamMessages = CreateNewStreamMessages(1, 2, 3);
            await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamMessages);
            var messageIdToDelete = newStreamMessages[1].MessageId;

            await store.DeleteMessage(streamId, messageIdToDelete);

            var page = await store.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);
            var message = page.Messages.Single();
            var messageDeleted = await message.GetJsonDataAs<MessageDeleted>();
            message.Type.ShouldBe(MessageDeletedMessageType);
            messageDeleted.StreamId.ShouldBe(streamId);
            messageDeleted.MessageId.ShouldBe(messageIdToDelete);
        }

        [Fact, Trait("Category", "DeleteEvent")]
        public async Task When_delete_message_that_does_not_exist_then_nothing_should_happen()
        {
            const string streamId = "stream";
            var newStreamMessages = CreateNewStreamMessages(1, 2, 3);
            await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamMessages);
            var initialHead = await store.ReadHeadPosition();

            await store.DeleteMessage(streamId, Guid.NewGuid());

            var page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 3);
            page.Messages.Length.ShouldBe(3);
            var subsequentHead = await store.ReadHeadPosition();
            subsequentHead.ShouldBe(initialHead);
        }

        [Fact, Trait("Category", "DeleteEvent")]
        public async Task When_delete_last_message_in_stream_and_append_then_it_should_have_subsequent_version_number()
        {
            const string streamId = "stream";
            var messages = CreateNewStreamMessages(1, 2, 3);
            await store.AppendToStream(streamId, ExpectedVersion.NoStream, messages);
            await store.DeleteMessage(streamId, messages.Last().MessageId);

            messages = CreateNewStreamMessages(4);
            await store.AppendToStream(streamId, 2, messages);

            var page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 3);
            page.Messages.Length.ShouldBe(3);
            page.LastStreamVersion.ShouldBe(3);
        }

        [Fact, Trait("Category", "DeleteEvent")]
        public async Task When_delete_a_messages_from_stream_with_then_can_read_all_forwards()
        {
            string streamId = "stream-1";
            await AppendMessages(store, streamId, 2);
            var page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 2);
            await store.DeleteMessage(streamId, page.Messages.First().MessageId);

            page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 2);

            page.Messages.Length.ShouldBe(1);
            page.LastStreamVersion.ShouldBe(1);
            page.NextStreamVersion.ShouldBe(2);
        }

        [Fact, Trait("Category", "DeleteEvent")]
        public async Task When_delete_all_messages_from_stream_with_1_messages_then_can_read_all_forwards()
        {
            string streamId = "stream-1";
            await AppendMessages(store, streamId, 1);
            var page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 2);
            await store.DeleteMessage(streamId, page.Messages[0].MessageId);

            page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 2);

            page.Messages.Length.ShouldBe(0);
            page.LastStreamVersion.ShouldBe(0);
            page.NextStreamVersion.ShouldBe(1);
        }

        [Fact, Trait("Category", "DeleteEvent")]
        public async Task When_delete_all_messages_from_stream_with_multiple_messages_then_can_read_all_forwards()
        {
            string streamId = "stream-1";
            await AppendMessages(store, streamId, 2);
            var page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 2);
            await store.DeleteMessage(streamId, page.Messages[0].MessageId);
            await store.DeleteMessage(streamId, page.Messages[1].MessageId);

            page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 2);

            page.Messages.Length.ShouldBe(0);
            page.LastStreamVersion.ShouldBe(1);
            page.NextStreamVersion.ShouldBe(2);
        }

        [Theory, Trait("Category", "DeleteEvent")]
        [InlineData("stream/id")]
        [InlineData("stream%id")]
        public async Task When_delete_stream_message_with_url_encodable_characters_then_should_not_throw(string streamId)
        {
            var newStreamMessages = CreateNewStreamMessages(1);
            await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamMessages);

            await store.DeleteMessage(streamId, newStreamMessages[0].MessageId);
        }
    }
}
