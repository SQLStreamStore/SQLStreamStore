namespace SqlStreamStore
{
    using System.Diagnostics;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class AcceptanceTests
    {
        [Fact]
        public async Task Time_to_take_to_read_1000_read_head_positions()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var stopwatch = Stopwatch.StartNew();

                    for(int i = 0; i < 1000; i++)
                    {
                        await store.ReadHeadPosition();
                    }

                    TestOutputHelper.WriteLine(stopwatch.ElapsedMilliseconds.ToString());
                }
            }
        }
    }
}