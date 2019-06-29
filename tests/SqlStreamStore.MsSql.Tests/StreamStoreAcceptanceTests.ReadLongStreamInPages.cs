namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using System.Linq;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class AcceptanceTests
    {
        [Fact]
        public async Task Given_large_message_stream_can_be_read_back_in_pages()
        {
            const string streamId = "stream-1";
            const int eventCount = 7500;

            var eventsToWrite = Enumerable.Range(0, eventCount)
                .Select(i => new NewStreamMessage(Guid.NewGuid(), $"message-{i}", "{}", $"{i}"))
                .ToArray();

            await store.AppendToStream(streamId, ExpectedVersion.NoStream, eventsToWrite);
            var result = store.ReadStreamForwards(streamId, StreamVersion.Start, 10, false);

            var readEvents = await result.ToArrayAsync();

            readEvents.Length.ShouldBe(eventsToWrite.Length);
        }
    }
}