namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Linq;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class ReadLongStreamInPagesAcceptanceTests : AcceptanceTests
    {
        public ReadLongStreamInPagesAcceptanceTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        { }

        [Fact]
        public async Task Given_large_message_stream_can_be_read_back_in_pages()
        {
            var eventsToWrite = CreateNewMessages();

            await store.AppendToStream("stream-1", ExpectedVersion.NoStream, eventsToWrite);

            var readEvents = await new PagedStreamStore(store).GetAsync("stream-1");

            readEvents.Count().ShouldBe(eventsToWrite.Length);
        }

        private static NewStreamMessage[] CreateNewMessages()
        {
            var eventsToWrite = new List<NewStreamMessage>();
            var largeStreamCount = 7500;
            for (int i = 0; i < largeStreamCount; i++)
            {
                var envelope = new NewStreamMessage(Guid.NewGuid(), $"message-{i}", "{}", $"{i}");

                eventsToWrite.Add(envelope);
            }

            return eventsToWrite.ToArray();
        }
    }

    public class PagedStreamStore
    {
        private readonly IStreamStore _streamStore;

        public PagedStreamStore(IStreamStore streamStore)
        {
            _streamStore = streamStore;
        }

        public async Task<IEnumerable<StreamMessage>> GetAsync(string streamName)
        {
            var start = 0;
            const int BatchSize = 500;

            ReadStreamPage page;
            var events = new List<StreamMessage>();

            do
            {
                page = await _streamStore.ReadStreamForwards(streamName, start, BatchSize);

                if (page.Status == PageReadStatus.StreamNotFound)
                {
                    throw new Exception("Stream not found");
                }

                events.AddRange(
                    page.Messages);

                start = page.NextStreamVersion;
            }
            while (!page.IsEnd);

            return events;
        }
    }
}
