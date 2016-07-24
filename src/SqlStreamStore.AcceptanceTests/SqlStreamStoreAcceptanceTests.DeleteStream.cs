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
        public async Task When_delete_stream_with_no_expected_version_and_read_then_should_get_StreamNotFound()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.DeleteStream(streamId);

                    var streamEventsPage =
                        await eventStore.ReadStreamForwards(streamId, StreamVersion.Start, 10);

                    streamEventsPage.Status.ShouldBe(PageReadStatus.StreamNotFound);
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_expected_version_any_and_then_read_then_should_stream_deleted_event()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.DeleteStream(streamId);

                    var streamEventsPage =
                        await eventStore.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);

                    streamEventsPage.Status.ShouldBe(PageReadStatus.Success);
                    var streamEvent = streamEventsPage.Events.Single();
                    streamEvent.Type.ShouldBe(StreamDeletedEventType);
                    var streamDeleted = streamEvent.JsonDataAs<StreamDeleted>();
                    streamDeleted.StreamId.ShouldBe("stream");
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_no_expected_version_and_read_all_then_should_not_see_deleted_stream_events()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.DeleteStream(streamId);

                    var allEventsPage = await eventStore.ReadAllForwards(Checkpoint.Start, 10);

                    allEventsPage.StreamEvents.Any(streamEvent => streamEvent.StreamId == streamId).ShouldBeFalse();
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_that_does_not_exist()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "notexist";
                    Func<Task> act = () => eventStore.DeleteStream(streamId);

                    act.ShouldNotThrow();
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_a_matching_expected_version_and_read_then_should_get_StreamNotFound()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.DeleteStream(streamId, 2);

                    var streamEventsPage =
                        await eventStore.ReadStreamForwards(streamId, StreamVersion.Start, 10);

                    streamEventsPage.Status.ShouldBe(PageReadStatus.StreamNotFound);
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_a_matching_expected_version_and_read_then_should_get_stream_deleted_event()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.DeleteStream(streamId, 2);

                    var streamEventsPage =
                        await eventStore.ReadStreamBackwards(DeletedStreamId, StreamVersion.End, 1);

                    streamEventsPage.Status.ShouldBe(PageReadStatus.Success);
                    var streamEvent = streamEventsPage.Events.Single();
                    streamEvent.Type.ShouldBe(StreamDeletedEventType);
                    var streamDeleted = streamEvent.JsonDataAs<StreamDeleted>();
                    streamDeleted.StreamId.ShouldBe("stream");
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_a_matching_expected_version_and_read_all_then_should_not_see_deleted_stream_events()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));
                    await eventStore.DeleteStream(streamId);

                    var allEventsPage = await eventStore.ReadAllForwards(Checkpoint.Start, 10);

                    allEventsPage.StreamEvents.Any(streamEvent => streamEvent.StreamId == streamId).ShouldBeFalse();
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_that_does_not_exist_then_should_not_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "notexist";

                    var exception = await Record.ExceptionAsync(() => eventStore.DeleteStream(streamId));

                    exception.ShouldBeNull();
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_that_does_not_exist_with_expected_version_number_then_should_not_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "notexist";
                    const int expectedVersion = 1;

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.DeleteStream(streamId, expectedVersion));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                        Messages.DeleteStreamFailedWrongExpectedVersion(streamId, expectedVersion));
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_a_non_matching_expected_version_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.DeleteStream(streamId, 100));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                            Messages.DeleteStreamFailedWrongExpectedVersion(streamId, 100));
                }
            }
        }
    }
}
