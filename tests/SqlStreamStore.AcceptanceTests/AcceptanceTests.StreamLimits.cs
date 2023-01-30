namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class AcceptanceTests<TReadAllPage>
    {
        [Theory, Trait("Category", "StreamMetadata"),
         InlineData(ExpectedVersion.NoStream), InlineData(ExpectedVersion.Any)]
        public async Task When_stream_has_max_count_and_append_exceeds_then_should_maintain_max_count(int expectedVersion)
        {
            const string streamId = "stream-1";
            const int maxCount = 2;
            await Store
                .SetStreamMetadata(streamId, maxCount: maxCount, metadataJson: "meta");
            await Store
                .AppendToStream(streamId, expectedVersion, CreateNewStreamMessages(1, 2, 3, 4));

            var streamMessagesPage = await Store.ReadStreamForwards(streamId, StreamVersion.Start, 4);

            streamMessagesPage.Messages.Length.ShouldBe(maxCount);
        }

        [Theory, Trait("Category", "StreamMetadata"),
         InlineData(ExpectedVersion.EmptyStream)]
        public async Task When_empty_stream_has_max_count_and_append_exceeds_then_should_maintain_max_count(int expectedVersion)
        {
            const string streamId = "stream-1";
            const int maxCount = 2;
            await Store
                .SetStreamMetadata(streamId, maxCount: maxCount, metadataJson: "meta");
            await Store
                .AppendToStream(streamId, ExpectedVersion.NoStream, new NewStreamMessage[0]);
            await Store
                .AppendToStream(streamId, expectedVersion, CreateNewStreamMessages(1, 2, 3, 4));

            var streamMessagesPage = await Store.ReadStreamForwards(streamId, StreamVersion.Start, 4);

            streamMessagesPage.Messages.Length.ShouldBe(maxCount);
        }

        [Theory, InlineData(1, 1), InlineData(2, 2), InlineData(6, 4), Trait("Category", "StreamMetadata")]
        public async Task When_stream_max_count_is_set_then_stream_should_have_max_count(
            int maxCount,
            int expectedLength)
        {
            const string streamId = "stream-1";

            await Store
                .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));
            await Store
                .SetStreamMetadata(streamId, maxCount: maxCount, metadataJson: "meta");

            var streamMessagesPage = await Store.ReadStreamForwards(streamId, StreamVersion.Start, 4);

            streamMessagesPage.Messages.Length.ShouldBe(expectedLength);
        }

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task When_stream_has_expired_messages_and_read_forwards_then_should_not_get_expired_messages()
        {
            var currentUtc = new DateTime(2016, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            Fixture.GetUtcNow = () => currentUtc;
            string streamId = "stream-1";
            await Store
                .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));
            currentUtc += TimeSpan.FromSeconds(60);
            await Store
                .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(5, 6, 7, 8));
            await Store
                .SetStreamMetadata(streamId, maxAge: 30, metadataJson: "meta");

            var streamMessagesPage = await Store.ReadStreamForwards(streamId, StreamVersion.Start, 8);

            streamMessagesPage.Messages.Length.ShouldBe(4);
        }

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task When_stream_has_expired_messages_and_read_backward_then_should_not_get_expired_messages()
        {
            var currentUtc = new DateTime(2016, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            Fixture.GetUtcNow = () => currentUtc;
            const string streamId = "stream-1";
            await Store
                .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));
            currentUtc += TimeSpan.FromSeconds(60);
            await Store
                .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(5, 6, 7, 8));
            await Store
                .SetStreamMetadata(streamId, maxAge: 30, metadataJson: "meta");

            var streamMessagesPage = await Store.ReadStreamBackwards(streamId, StreamVersion.End, 8);

            streamMessagesPage.Messages.Length.ShouldBe(4);
        }

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task When_streams_have_expired_messages_and_read_all_forwards_then_should_not_get_expired_messages()
        {
            var currentUtc = new DateTime(2016, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            Fixture.GetUtcNow = () => currentUtc;
            // Arrange
            const string streamId1 = "stream-1";
            const string streamId2 = "streamId-2";
            await Store
                .AppendToStream(streamId1, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));
            await Store
                .AppendToStream(streamId2, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));

            currentUtc += TimeSpan.FromSeconds(60);

            await Store
                .AppendToStream(streamId1, ExpectedVersion.Any, CreateNewStreamMessages(5, 6));
            await Store
                .AppendToStream(streamId2, ExpectedVersion.Any, CreateNewStreamMessages(5, 6, 7, 8));

            await Store
                .SetStreamMetadata(streamId1, maxAge: 30, metadataJson: "meta");
            await Store
                .SetStreamMetadata(streamId2, maxAge: 30, metadataJson: "meta");

            // Act
            var allMessagesPage = await Store.ReadAllForwards(Position.Start, 20);

            // Assert
            allMessagesPage.Messages.Count(message => message.StreamId == streamId1).ShouldBe(2);
            allMessagesPage.Messages.Count(message => message.StreamId == streamId2).ShouldBe(4);
        }

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task When_streams_have_expired_messages_and_read_all_backwards_then_should_not_get_expired_messages()
        {
            var currentUtc = new DateTime(2016, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            Fixture.GetUtcNow = () => currentUtc;

            // Arrange
            const string streamId1 = "stream-1";
            const string streamId2 = "streamId-2";
            await Store
                .AppendToStream(streamId1, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));
            await Store
                .AppendToStream(streamId2, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));

            currentUtc += TimeSpan.FromSeconds(60);

            await Store
                .AppendToStream(streamId1, ExpectedVersion.Any, CreateNewStreamMessages(5, 6));
            await Store
                .AppendToStream(streamId2, ExpectedVersion.Any, CreateNewStreamMessages(5, 6, 7, 8));

            await Store
                .SetStreamMetadata(streamId1, maxAge: 30, metadataJson: "meta");
            await Store
                .SetStreamMetadata(streamId2, maxAge: 30, metadataJson: "meta");

            // Act
            var allMessagesPage = await Store.ReadAllBackwards(Position.End, 20);

            // Assert
            allMessagesPage.Messages.Count(message => message.StreamId == streamId1).ShouldBe(2);
            allMessagesPage.Messages.Count(message => message.StreamId == streamId2).ShouldBe(4);
        }
    }
}