namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using static Streams.Deleted;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task When_delete_event_then_event_should_be_removed_from_stream()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream";
                    var newStreamEvents = CreateNewStreamMessages(1, 2, 3);
                    await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvents);
                    var eventIdToDelete = newStreamEvents[1].EventId;

                    await store.DeleteMessage(streamId, eventIdToDelete);

                    var streamEventsPage = await store.ReadStreamForwards(streamId, StreamVersion.Start, 3);

                    streamEventsPage.Messages.Length.ShouldBe(2);
                    streamEventsPage.Messages.Any(e => e.EventId == eventIdToDelete).ShouldBeFalse();
                }
            }
        }

        [Fact]
        public async Task When_delete_event_then_deleted_event_should_be_appended_to_deleted_stream()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream";
                    var newStreamEvents = CreateNewStreamMessages(1, 2, 3);
                    await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvents);
                    var eventIdToDelete = newStreamEvents[1].EventId;

                    await store.DeleteMessage(streamId, eventIdToDelete);

                    var streamEventsPage = await store.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);
                    var streamEvent = streamEventsPage.Messages.Single();
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
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream";
                    var newStreamEvents = CreateNewStreamMessages(1, 2, 3);
                    await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvents);
                    var initialHead = await store.ReadHeadCheckpoint();

                    await store.DeleteMessage(streamId, Guid.NewGuid());

                    var streamEventsPage = await store.ReadStreamForwards(streamId, StreamVersion.Start, 3);
                    streamEventsPage.Messages.Length.ShouldBe(3);
                    var subsequentHead = await store.ReadHeadCheckpoint();
                    subsequentHead.ShouldBe(initialHead);
                }
            }
        }

        [Fact]
        public async Task When_delete_last_event_in_stream_and_append_then_it_should_have_subsequent_version_number()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream";
                    var newStreamEvents = CreateNewStreamMessages(1, 2, 3);
                    await store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvents);
                    await store.DeleteMessage(streamId, newStreamEvents.Last().EventId);

                    newStreamEvents = CreateNewStreamMessages(4);
                    await store.AppendToStream(streamId, 2, newStreamEvents);

                    var streamEventsPage = await store.ReadStreamForwards(streamId, StreamVersion.Start, 3);
                    streamEventsPage.Messages.Length.ShouldBe(3);
                    streamEventsPage.LastStreamVersion.ShouldBe(3);
                }
            }
        }
    }
}
