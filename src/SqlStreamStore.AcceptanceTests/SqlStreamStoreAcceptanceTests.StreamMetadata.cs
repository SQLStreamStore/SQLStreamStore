namespace SqlStreamStore
{
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        //TODO: Port some of the tests from AppendStream with regard to expected version to verify behavior of Get/SetStreamMetadata.

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task When_get_non_existent_metadata_then_meta_stream_version_should_be_negative()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    var metadata = await store.GetStreamMetadata(streamId);

                    metadata.StreamId.ShouldBe(streamId);
                    metadata.MaxAge.ShouldBeNull();
                    metadata.MetadataStreamVersion.ShouldBeLessThan(0);
                    metadata.MaxCount.ShouldBeNull();
                    metadata.MetadataJson.ShouldBeNull();
                }
            }
        }

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task Can_set_and_get_stream_metadata_for_non_existent_stream()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
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
        public async Task Can_set_and_get_stream_metadata_for_non_existent_stream_and_append()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    var streamId = "stream-1";

                    await store.SetStreamMetadata(streamId, maxCount: 1, maxAge: 2);

                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var metadataResult = await store.GetStreamMetadata(streamId);

                    metadataResult.MaxCount.ShouldBe(1);
                    metadataResult.MaxAge.ShouldBe(2);
                }
            }
        }

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task Can_set_and_get_stream_metadata_after_stream_is_created()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

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

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task When_delete_stream_with_metadata_then_meta_data_stream_is_deleted()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    const string streamId = "059846C3-6701-45E9-A72A-20986539D4D3";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store
                        .SetStreamMetadata(streamId, maxCount: 3, metadataJson: "meta");

                    await store.DeleteStream(streamId);

                    var allMessagesPage = await store.ReadAllForwards(Position.Start, 10);
                    var streamDeletedMessages = allMessagesPage.Messages
                        .Where(m => m.Type == Deleted.StreamDeletedMessageType)
                        .ToArray();
                    streamDeletedMessages.Length.ShouldBe(2);

                    var streamDeleted1 = await streamDeletedMessages[0].GetJsonDataAs<Deleted.StreamDeleted>();
                    streamDeleted1.StreamId.ShouldBe(streamId);

                    var streamDeleted2 = await streamDeletedMessages[1].GetJsonDataAs<Deleted.StreamDeleted>();
                    streamDeleted2.StreamId.ShouldBe($"$${streamId}");
                }
            }
        }

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task When_set_metadata_with_same_data_then_should_handle_idempotently()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .SetStreamMetadata(streamId, maxCount: 2, maxAge: 30, metadataJson: "meta");
                    await store
                        .SetStreamMetadata(streamId, maxCount: 2, maxAge: 30, metadataJson: "meta");

                    var metadata = await store.GetStreamMetadata(streamId);

                    metadata.MetadataStreamVersion.ShouldBe(0);
                }
            }
        }

        [Fact, Trait("Category", "StreamMetadata")]
        public async Task Can_set_deleted_stream_metadata()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = Deleted.DeletedStreamId;
                    await store
                        .SetStreamMetadata(streamId, maxCount: 2, maxAge: 30);

                    var metadata = await store.GetStreamMetadata(streamId);

                    metadata.MetadataStreamVersion.ShouldBe(0);
                    metadata.MaxAge.ShouldBe(30);
                    metadata.MaxCount.ShouldBe(2);
                    metadata.MetadataJson.ShouldBeNull();
                }
            }
        }
    }
}
