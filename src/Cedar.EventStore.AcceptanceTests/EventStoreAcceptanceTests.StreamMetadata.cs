namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_set_and_get_stream_metadata()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    string streamId = Guid.NewGuid().ToString();
                    await eventStore
                        .SetStreamMetadata(streamId, maxAge: 2, maxCount: 3, metadataJson: "meta");

                    var metadata = await eventStore.GetStreamMetadata(streamId);

                    metadata.StreamId.ShouldBe(streamId);
                    metadata.MaxAge.ShouldBe(2);
                    metadata.MetadataStreamVersion.ShouldBe(0);
                    metadata.MaxCount.ShouldBe(3);
                    metadata.MetadataJson.ShouldBe("meta");
                }
            }
        }

        [Fact]
        public async  Task When_delete_stream_with_metadata_then_meta_data_stream_is_deleted()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    string streamId = "059846C3-6701-45E9-A72A-20986539D4D3";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore
                        .SetStreamMetadata(streamId, maxAge: 2, maxCount: 3, metadataJson: "meta");

                    await eventStore.DeleteStream(streamId);

                    var allEventsPage = await eventStore.ReadAllForwards(Checkpoint.Start, 10);
                    allEventsPage.StreamEvents.Length.ShouldBe(2);
                    allEventsPage.StreamEvents[0].Type.ShouldBe(Deleted.StreamDeletedEventType);
                    allEventsPage.StreamEvents[0].JsonDataAs<Deleted.StreamDeleted>()
                        .StreamId.ShouldBe(streamId);

                    allEventsPage.StreamEvents[1].Type.ShouldBe(Deleted.StreamDeletedEventType);
                    allEventsPage.StreamEvents[1].JsonDataAs<Deleted.StreamDeleted>()
                        .StreamId.ShouldBe($"$${streamId}");
                }
            }
        }
    }
}
