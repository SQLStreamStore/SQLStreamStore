namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
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
    }
}
