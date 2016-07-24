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
                using (var eventStore = await fixture.GetEventStore())
                {
                    var eventsToWrite = CreateNewStreamEvents();

                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, eventsToWrite);


                    var readEvents = await new PagedEventStore(eventStore).GetAsync("stream-1");

                    readEvents.Count().ShouldBe(eventsToWrite.Length);
                }
            }
        }

        private static NewStreamEvent[] CreateNewStreamEvents()
        {
            var eventsToWrite = new List<NewStreamEvent>();
            var largeStreamCount = 7500;
            for (int i = 0; i < largeStreamCount; i++)
            {
                var envelope = new NewStreamEvent(Guid.NewGuid(), $"event{i}", "{}", $"{i}");

                eventsToWrite.Add(envelope);
            }

            return eventsToWrite.ToArray();
        }
    }

    public class PagedEventStore
    {
        private readonly IEventStore _eventStore;

        public PagedEventStore(IEventStore eventStore)
        {
            _eventStore = eventStore;
        }

        public async Task<IEnumerable<StreamEvent>> GetAsync(string streamName)
        {
            var start = 0;
            const int BatchSize = 500;

            StreamEventsPage eventsPage;
            var events = new List<StreamEvent>();

            do
            {
                eventsPage = await _eventStore.ReadStreamForwards(streamName, start, BatchSize);

                if (eventsPage.Status == PageReadStatus.StreamNotFound)
                {
                    throw new Exception("Stream not found");
                }

                events.AddRange(
                    eventsPage.Events);

                start = eventsPage.NextStreamVersion;
            }
            while (!eventsPage.IsEndOfStream);

            return events;
        }
    }
}
