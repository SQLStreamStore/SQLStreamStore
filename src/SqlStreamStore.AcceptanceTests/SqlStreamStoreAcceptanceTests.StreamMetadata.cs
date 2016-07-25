namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task When_get_non_existent_metadata_then_meta_stream_version_should_be_negative()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = Guid.NewGuid().ToString();

                    var metadata = await store.GetStreamMetadata(streamId);

                    metadata.StreamId.ShouldBe(streamId);
                    metadata.MaxAge.ShouldBeNull();
                    metadata.MetadataStreamVersion.ShouldBeLessThan(0);
                    metadata.MaxCount.ShouldBeNull();
                    metadata.MetadataJson.ShouldBeNull();
                }
            }
        }

        [Fact]
        public async Task Can_set_and_get_stream_metadata_for_non_existent_stream()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    var streamId = "stream-1";
                    await store
                        .SetStreamMetadata(streamId, maxAge: 2, maxCount: 3, metadataJson: "meta");

                    var metadata = await store.GetStreamMetadata(streamId);

                    metadata.StreamId.ShouldBe(streamId);
                    metadata.MaxAge.ShouldBe(2);
                    metadata.MetadataStreamVersion.ShouldBe(0);
                    metadata.MaxCount.ShouldBe(3);
                    metadata.MetadataJson.ShouldBe("meta");
                }
            }
        }

        [Fact]
        public async Task Can_set_and_get_stream_metadata_after_stream_is_created()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";

                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessageSequence(1, 4));

                    await store
                        .SetStreamMetadata(streamId, maxAge: 2, maxCount: 3, metadataJson: "meta");

                    var metadata = await store.GetStreamMetadata(streamId);

                    metadata.StreamId.ShouldBe(streamId);
                    metadata.MaxAge.ShouldBe(2);
                    metadata.MetadataStreamVersion.ShouldBe(0);
                    metadata.MaxCount.ShouldBe(3);
                    metadata.MetadataJson.ShouldBe("meta");
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_metadata_then_meta_data_stream_is_deleted()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    string streamId = "059846C3-6701-45E9-A72A-20986539D4D3";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store
                        .SetStreamMetadata(streamId, maxCount: 3, metadataJson: "meta");

                    await store.DeleteStream(streamId);

                    var allMessagesPage = await store.ReadAllForwards(Position.Start, 10);
                    allMessagesPage.Messages.Length.ShouldBe(2);
                    allMessagesPage.Messages[0].Type.ShouldBe(Deleted.StreamDeletedMessageType);
                    allMessagesPage.Messages[0].JsonDataAs<Deleted.StreamDeleted>()
                        .StreamId.ShouldBe(streamId);

                    allMessagesPage.Messages[1].Type.ShouldBe(Deleted.StreamDeletedMessageType);
                    allMessagesPage.Messages[1].JsonDataAs<Deleted.StreamDeleted>()
                        .StreamId.ShouldBe($"$${streamId}");
                }
            }
        }

        [Fact]
        public async Task When_stream_has_max_count_and_append_exceeds_then_should_maintain_max_count()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    int maxCount = 2;
                    await store
                        .SetStreamMetadata(streamId, maxCount: maxCount, metadataJson: "meta");
                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3, 4));

                    var streamMessagesPage = await store.ReadStreamForwards(streamId, StreamVersion.Start, 4);

                    streamMessagesPage.Messages.Length.ShouldBe(maxCount);
                }
            }
        }

        [Fact]
        public async Task When_stream_max_count_is_set_then_stream_should_have_max_count()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    int maxCount = 2;

                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));
                    await store
                        .SetStreamMetadata(streamId, maxCount: maxCount, metadataJson: "meta");

                    var streamMessagesPage = await store.ReadStreamForwards(streamId, StreamVersion.Start, 4);

                    streamMessagesPage.Messages.Length.ShouldBe(maxCount);
                }
            }
        }

        [Fact]
        public async Task When_stream_has_expired_messages_and_read_forwards_then_should_not_get_expired_messages()
        {
            using (var fixture = GetFixture())
            {
                var currentUtc = new DateTime(2016, 1, 1, 0, 0, 0);
                fixture.GetUtcNow = () => currentUtc;
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));
                    currentUtc += TimeSpan.FromSeconds(60);
                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(5, 6, 7, 8));
                    await store
                        .SetStreamMetadata(streamId, maxAge: 30, metadataJson: "meta");

                    var streamMessagesPage = await store.ReadStreamForwards(streamId, StreamVersion.Start, 8);

                    streamMessagesPage.Messages.Length.ShouldBe(4);
                }
            }
        }

        [Fact]
        public async Task When_stream_has_expired_messages_and_read_backward_then_should_not_get_expired_messages()
        {
            using (var fixture = GetFixture())
            {
                var currentUtc = new DateTime(2016, 1, 1, 0, 0, 0);
                fixture.GetUtcNow = () => currentUtc;
                using (var store = await fixture.GetStreamStore())
                {
                    string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));
                    currentUtc += TimeSpan.FromSeconds(60);
                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(5, 6, 7, 8));
                    await store
                        .SetStreamMetadata(streamId, maxAge: 30, metadataJson: "meta");

                    var streamMessagesPage = await store.ReadStreamBackwards(streamId, StreamVersion.End, 8);

                    streamMessagesPage.Messages.Length.ShouldBe(4);
                }
            }
        }

        [Fact]
        public async Task When_streams_have_expired_messages_and_read_all_forwards_then_should_not_get_expired_messages()
        {
            using (var fixture = GetFixture())
            {
                var currentUtc = new DateTime(2016, 1, 1, 0, 0, 0);
                fixture.GetUtcNow = () => currentUtc;
                using (var store = await fixture.GetStreamStore())
                {
                    // Arrange
                    string streamId1 = "stream-1", streamId2 = "streamId-2";
                    await store
                        .AppendToStream(streamId1, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));
                    await store
                        .AppendToStream(streamId2, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));

                    currentUtc += TimeSpan.FromSeconds(60);

                    await store
                        .AppendToStream(streamId1, ExpectedVersion.Any, CreateNewStreamMessages(5, 6));
                    await store
                        .AppendToStream(streamId2, ExpectedVersion.Any, CreateNewStreamMessages(5, 6, 7, 8));

                    await store
                        .SetStreamMetadata(streamId1, maxAge: 30, metadataJson: "meta");
                    await store
                        .SetStreamMetadata(streamId2, maxAge: 30, metadataJson: "meta");

                    // Act
                    var allMessagesPage = await store.ReadAllForwards(Position.Start, 20);

                    // Assert
                    allMessagesPage.Messages.Where(message => message.StreamId == streamId1).Count().ShouldBe(2);
                    allMessagesPage.Messages.Where(message => message.StreamId == streamId2).Count().ShouldBe(4);
                }
            }
        }

        [Fact]
        public async Task When_streams_have_expired_messages_and_read_all_backwards_then_should_not_get_expired_messages()
        {
            using (var fixture = GetFixture())
            {
                var currentUtc = new DateTime(2016, 1, 1, 0, 0, 0);
                fixture.GetUtcNow = () => currentUtc;
                using (var store = await fixture.GetStreamStore())
                {
                    // Arrange
                    string streamId1 = "stream-1", streamId2 = "streamId-2";
                    await store
                        .AppendToStream(streamId1, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));
                    await store
                        .AppendToStream(streamId2, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3, 4));

                    currentUtc += TimeSpan.FromSeconds(60);

                    await store
                        .AppendToStream(streamId1, ExpectedVersion.Any, CreateNewStreamMessages(5, 6));
                    await store
                        .AppendToStream(streamId2, ExpectedVersion.Any, CreateNewStreamMessages(5, 6, 7, 8));

                    await store
                        .SetStreamMetadata(streamId1, maxAge: 30, metadataJson: "meta");
                    await store
                        .SetStreamMetadata(streamId2, maxAge: 30, metadataJson: "meta");

                    // Act
                    var allMessagesPage = await store.ReadAllBackwards(Position.End, 20);

                    // Assert
                    allMessagesPage.Messages.Where(message => message.StreamId == streamId1).Count().ShouldBe(2);
                    allMessagesPage.Messages.Where(message => message.StreamId == streamId2).Count().ShouldBe(4);
                }
            }
        }
    }
}
