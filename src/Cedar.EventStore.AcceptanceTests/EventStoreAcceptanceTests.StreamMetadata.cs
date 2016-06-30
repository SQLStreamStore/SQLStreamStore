namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_get_stream_metadata()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    var metadata = await eventStore.GetStreamMetadata(streamId);


                }
            }
        }
    }
}
