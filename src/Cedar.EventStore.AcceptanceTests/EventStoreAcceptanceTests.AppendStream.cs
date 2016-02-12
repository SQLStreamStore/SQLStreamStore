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

                    var exception = await Record.ExceptionAsync(() => eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(2, 3, 4)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                        Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, ExpectedVersion.NoStream));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_same_events_then_should_then_should_be_idempotent()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2));

                    var exception = await Record.ExceptionAsync(() => eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2)));

                    exception.ShouldBeNull();
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

                    var exception = await Record.ExceptionAsync(() => 
                        eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                        Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, ExpectedVersion.NoStream));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_same_inital_event_then_should_be_idempotent()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2));

                    var exception = await Record.ExceptionAsync(() =>
                       eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1)));

                    exception.ShouldBeNull();
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

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(2)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
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

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.AppendToStream(streamId, 1, CreateNewStreamEvents(4, 5, 6)));
                    
                    exception.ShouldBeOfType<WrongExpectedVersionException>(
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

                    var exception = await Record.ExceptionAsync(() => 
                        eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5, 6)));

                    exception.ShouldBeNull();
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

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5)));

                    exception.ShouldBeNull();
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

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(4, 5, 6, 7)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
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
                    var exception = await Record.ExceptionAsync(() => 
                        eventStore.AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2, 3)));
                    exception.ShouldBeNull();

                    var page = await eventStore
                        .ReadStream(streamId, StreamVersion.Start, 4);
                    page.Events.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_expected_version_any_and_all_events_comitted_then_should_be_idempotent()
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
                        .ReadStream(streamId, StreamVersion.Start, 10);
                    page.Events.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_expected_version_any_and_some_of_the_events_previously_comitted_then_should_be_idempotent()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2));

                    var page = await eventStore
                        .ReadStream(streamId, StreamVersion.Start, 10);
                    page.Events.Length.ShouldBe(3);
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
                        .ReadStream(streamId, StreamVersion.Start, 10);
                    page.Events.Length.ShouldBe(6);
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_expected_version_any_and_some_of_the_events_previously_comitted_and_with_additional_events_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1, 2, 3));

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(2, 3, 4)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, ExpectedVersion.Any));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_expected_version_and_duplicate_event_Id_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream-1";
                    await eventStore
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.AppendToStream(streamId, 2, CreateNewStreamEvents(1)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, 2));
                }
            }
        }
    }
}
