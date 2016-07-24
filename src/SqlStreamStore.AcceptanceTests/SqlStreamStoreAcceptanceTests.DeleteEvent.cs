namespace StreamStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using StreamStore.Streams;
    using Shouldly;
    using Xunit;
    using static StreamStore.Streams.Deleted;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task When_delete_event_then_event_should_be_removed_from_stream()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    var newStreamEvents = CreateNewStreamEvents(1, 2, 3);
                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvents);
                    var eventIdToDelete = newStreamEvents[1].EventId;

                    await eventStore.DeleteEvent(streamId, eventIdToDelete);

                    var streamEventsPage = await eventStore.ReadStreamForwards(streamId, StreamVersion.Start, 3);

                    streamEventsPage.Events.Length.ShouldBe(2);
                    streamEventsPage.Events.Any(e => e.EventId == eventIdToDelete).ShouldBeFalse();
                }
            }
        }

        [Fact]
        public async Task When_delete_event_then_deleted_event_should_be_appended_to_deleted_stream()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    var newStreamEvents = CreateNewStreamEvents(1, 2, 3);
                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvents);
                    var eventIdToDelete = newStreamEvents[1].EventId;

                    await eventStore.DeleteEvent(streamId, eventIdToDelete);

                    var streamEventsPage = await eventStore.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);
                    var streamEvent = streamEventsPage.Events.Single();
                    var eventDeleted = streamEvent.JsonDataAs<EventDeleted>();
                    streamEvent.Type.ShouldBe(EventDeletedEventType);
                    eventDeleted.StreamId.ShouldBe(streamId);
                    eventDeleted.EventId.ShouldBe(eventIdToDelete);
                }
            }
        }

        [Fact]
        public async Task When_delete_event_that_does_not_exist_then_nothing_should_happen()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    var newStreamEvents = CreateNewStreamEvents(1, 2, 3);
                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvents);
                    var initialHead = await eventStore.ReadHeadCheckpoint();

                    await eventStore.DeleteEvent(streamId, Guid.NewGuid());

                    var streamEventsPage = await eventStore.ReadStreamForwards(streamId, StreamVersion.Start, 3);
                    streamEventsPage.Events.Length.ShouldBe(3);
                    var subsequentHead = await eventStore.ReadHeadCheckpoint();
                    subsequentHead.ShouldBe(initialHead);
                }
            }
        }

        [Fact]
        public async Task When_delete_last_event_in_stream_and_append_then_it_should_have_subsequent_version_number()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    var newStreamEvents = CreateNewStreamEvents(1, 2, 3);
                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvents);
                    await eventStore.DeleteEvent(streamId, newStreamEvents.Last().EventId);

                    newStreamEvents = CreateNewStreamEvents(4);
                    await eventStore.AppendToStream(streamId, 2, newStreamEvents);

                    var streamEventsPage = await eventStore.ReadStreamForwards(streamId, StreamVersion.Start, 3);
                    streamEventsPage.Events.Length.ShouldBe(3);
                    streamEventsPage.LastStreamVersion.ShouldBe(3);
                }
            }
        }
    }
}
