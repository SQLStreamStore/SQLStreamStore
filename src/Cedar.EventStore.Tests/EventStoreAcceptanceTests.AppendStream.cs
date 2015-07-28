namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using Cedar.EventStore.Exceptions;
    using Xunit;

    public abstract partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_different_event_then_should_throw()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(2, 3, 4))
                        .ShouldThrow<WrongExpectedVersionException>(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, ExpectedVersion.NoStream));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_same_events_then_should_not_throw()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3))
                        .ShouldNotThrow();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_same_inital_event_then_should_not_throw()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2))
                        .ShouldNotThrow();
                }
            }
        }

        [Fact]
        public async Task When_append_with_wrong_expected_version_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream(streamId, 1, CreateNewStreamEvents(4, 5, 6))
                        .ShouldThrow<WrongExpectedVersionException>(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, 1));
                }
            }
        }

        [Fact]
        public async Task Can_append_stream_with_correct_expected_version()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5, 6));
                }
            }
        }
    }
}
