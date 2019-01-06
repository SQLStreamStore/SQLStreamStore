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
        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_with_no_expected_version_and_read_then_should_get_StreamNotFound()
        {
            const string streamId = "stream";

            await store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
            await store.DeleteStream(streamId);

            var page =
                await store.ReadStreamForwards(streamId, StreamVersion.Start, 10);

            page.Status.ShouldBe(PageReadStatus.StreamNotFound);
        }

        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_with_expected_version_any_and_then_read_then_should_stream_deleted_message()
        {
            const string streamId = "stream";

            await store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
            await store.DeleteStream(streamId, 2);

            var page =
                await store.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);

            page.Status.ShouldBe(PageReadStatus.Success);
            var message = page.Messages.Single();
            message.Type.ShouldBe(StreamDeletedMessageType);
            var streamDeleted = await message.GetJsonDataAs<StreamDeleted>();
            streamDeleted.StreamId.ShouldBe("stream");
        }

        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_with_no_expected_version_and_read_all_then_should_not_see_deleted_stream_messages()
        {
            const string streamId = "stream";

            await store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
            await store.DeleteStream(streamId);

            var page = await store.ReadAllForwards(Position.Start, 10);

            page.Messages.Any(message => message.StreamId == streamId).ShouldBeFalse();
        }

        [Fact, Trait("Category", "DeleteStream")]
        public void When_delete_stream_that_does_not_exist()
        {
            const string streamId = "notexist";
            Func<Task> act = () => store.DeleteStream(streamId);

            act.ShouldNotThrow();
        }

        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_with_a_matching_expected_version_and_read_then_should_get_StreamNotFound()
        {
            const string streamId = "stream";

            await store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
            await store.DeleteStream(streamId, 2);

            var page =
                await store.ReadStreamForwards(streamId, StreamVersion.Start, 10);

            page.Status.ShouldBe(PageReadStatus.StreamNotFound);
        }

        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_with_a_matching_expected_version_and_read_then_should_get_stream_deleted_message()
        {
            const string streamId = "stream";

            await store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
            await store.DeleteStream(streamId, 2);

            var page =
                await store.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);

            page.Status.ShouldBe(PageReadStatus.Success);
            var message = page.Messages.Single();
            message.Type.ShouldBe(StreamDeletedMessageType);
            var streamDeleted = await message.GetJsonDataAs<StreamDeleted>();
            streamDeleted.StreamId.ShouldBe("stream");
        }

        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_with_a_matching_expected_version_and_read_all_then_should_not_see_deleted_stream_messages()
        {
            const string streamId = "stream";

            await store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
            await store.DeleteStream(streamId);

            var allMessagesPage = await store.ReadAllForwards(Position.Start, 10);

            allMessagesPage.Messages.Any(message => message.StreamId == streamId).ShouldBeFalse();
        }

        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_that_does_not_exist_then_should_not_throw()
        {
            const string streamId = "notexist";

            var exception = await Record.ExceptionAsync(() => store.DeleteStream(streamId));

            exception.ShouldBeNull();
        }

        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_that_does_not_exist_with_expected_version_number_then_should_not_throw()
        {
            const string streamId = "notexist";
            const int expectedVersion = 1;

            var exception = await Record.ExceptionAsync(() =>
                store.DeleteStream(streamId, expectedVersion));

            exception.ShouldBeOfType<WrongExpectedVersionException>(
                ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId, expectedVersion));
        }

        [Fact, Trait("Category", "DeleteStream")]
        public async Task When_delete_stream_with_a_non_matching_expected_version_then_should_throw()
        {
            const string streamId = "stream";
            await store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

            var exception = await Record.ExceptionAsync(() =>
                store.DeleteStream(streamId, 100));

            exception.ShouldBeOfType<WrongExpectedVersionException>(
                    ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId, 100));
        }
    }
}
