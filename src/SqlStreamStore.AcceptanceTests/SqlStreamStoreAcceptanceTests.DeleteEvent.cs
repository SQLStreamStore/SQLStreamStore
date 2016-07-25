namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using static Streams.Deleted;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task When_delete_message_then_message_should_be_removed_from_stream()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
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
            }
        }

        [Fact]
        public async Task When_delete_message_then_deleted_message_should_be_appended_to_deleted_stream()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream";
                    var newStreamMessages = CreateNewStreamMessages(1, 2, 3);
                    await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamMessages);
                    var messageIdToDelete = newStreamMessages[1].MessageId;

                    await store.DeleteMessage(streamId, messageIdToDelete);

                    var page = await store.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);
                    var message = page.Messages.Single();
                    var messageDeleted = message.JsonDataAs<MessageDeleted>();
                    message.Type.ShouldBe(MessageDeletedMessageType);
                    messageDeleted.StreamId.ShouldBe(streamId);
                    messageDeleted.MessageId.ShouldBe(messageIdToDelete);
                }
            }
        }

        [Fact]
        public async Task When_delete_message_that_does_not_exist_then_nothing_should_happen()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
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
            }
        }

        [Fact]
        public async Task When_delete_last_message_in_stream_and_append_then_it_should_have_subsequent_version_number()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
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
            }
        }
    }
}
