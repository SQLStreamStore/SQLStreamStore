namespace StreamStore
{
    using System.Diagnostics;
    using System.Threading.Tasks;
    using StreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task Time_to_take_to_read_1000_read_head_checkpoints()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    var stopwatch = Stopwatch.StartNew();

                    for(int i = 0; i < 1000; i++)
                    {
                        await eventStore.ReadHeadCheckpoint();
                    }

                    _testOutputHelper.WriteLine(stopwatch.ElapsedMilliseconds.ToString());
                }
            }
        }
    }
}