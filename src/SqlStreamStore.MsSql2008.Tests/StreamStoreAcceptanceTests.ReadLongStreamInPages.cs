namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Linq;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task Given_large_event_stream_can_be_read_back_in_pages()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    var eventsToWrite = CreateNewStreamEvents();

                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, eventsToWrite);

                    var readEvents = await new PagedStreamStore(store).GetAsync("stream-1");

                    readEvents.Count().ShouldBe(eventsToWrite.Length);
                }
            }
        }

        private static NewStreamMessage[] CreateNewStreamEvents()
        {
            var eventsToWrite = new List<NewStreamMessage>();
            var largeStreamCount = 7500;
            for (int i = 0; i < largeStreamCount; i++)
            {
                var envelope = new NewStreamMessage(Guid.NewGuid(), $"event{i}", "{}", $"{i}");

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

            StreamMessagesPage messagesPage;
            var events = new List<StreamMessage>();

            do
            {
                messagesPage = await _streamStore.ReadStreamForwards(streamName, start, BatchSize);

                if (messagesPage.Status == PageReadStatus.StreamNotFound)
                {
                    throw new Exception("Stream not found");
                }

                events.AddRange(
                    messagesPage.Messages);

                start = messagesPage.NextStreamVersion;
            }
            while (!messagesPage.IsEndOfStream);

            return events;
        }
    }
}
