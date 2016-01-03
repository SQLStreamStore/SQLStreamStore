namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
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
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2))
                        .ShouldNotThrow();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_additonal_events_then_should_throw()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3))
                        .ShouldThrow<WrongExpectedVersionException>(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, ExpectedVersion.NoStream));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_same_inital_events_then_should_not_throw()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1))
                        .ShouldNotThrow();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_different_inital_events_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(2))
                        .ShouldThrow<WrongExpectedVersionException>(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, ExpectedVersion.NoStream));
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

        [Fact]
        public async Task When_append_stream_with_correct_expected_version_second_time_with_same_events_then_should_not_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5, 6));

                    await eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5, 6))
                        .ShouldNotThrow();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_correct_expected_version_second_time_with_same_initial_events_then_should_not_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5, 6));

                    await eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5))
                        .ShouldNotThrow();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_correct_expected_version_second_time_with_additional_events_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5, 6));

                    await eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5, 6, 7))
                        .ShouldThrow<WrongExpectedVersionException>(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, 2));
                }
            }
        }

        [Fact]
        public async Task Can_append_to_non_existing_stream_with_expected_version_any()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2, 3))
                        .ShouldNotThrow();

                    var page = await eventStore
                        .ReadStream(streamId, StreamPosition.Start, 4);
                    page.Events.Count.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task Can_append_stream_with_expected_version_any_and_all_events_comitted_already()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2, 3));

                    var page = await eventStore
                        .ReadStream(streamId, StreamPosition.Start, 10);
                    page.Events.Count.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task Can_append_stream_with_expected_version_any_and_none_of_then_events_previously_comitted()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(4, 5, 6));

                    var page = await eventStore
                        .ReadStream(streamId, StreamPosition.Start, 10);
                    page.Events.Count.ShouldBe(6);
                }
            }
        }

        [Fact]
        public async Task Can_append_stream_with_expected_version_any_and_some_of_then_events_previously_comitted()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(2, 3, 4))
                        .ShouldThrow<WrongExpectedVersionException>(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, ExpectedVersion.Any));
                }
            }
        }
    }
}
