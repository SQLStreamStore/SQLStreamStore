namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_different_message_then_should_throw()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var exception = await Record.ExceptionAsync(() => store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(2, 3, 4)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId, ExpectedVersion.NoStream));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_same_messages_then_should_then_should_be_idempotent()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));

                    var exception = await Record.ExceptionAsync(() => store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2)));

                    exception.ShouldBeNull();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_additonal_messages_then_should_throw()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));

                    var exception = await Record.ExceptionAsync(() => 
                        store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId, ExpectedVersion.NoStream));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_same_inital_message_then_should_be_idempotent()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));

                    var exception = await Record.ExceptionAsync(() =>
                       store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1)));

                    exception.ShouldBeNull();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_different_inital_messages_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));

                    var exception = await Record.ExceptionAsync(() =>
                        store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(2)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamId, ExpectedVersion.NoStream));
                }
            }
        }

        [Fact]
        public async Task When_append_with_wrong_expected_version_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var exception = await Record.ExceptionAsync(() =>
                        store.AppendToStream(streamId, 1, CreateNewStreamMessages(4, 5, 6)));
                    
                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId, 1));
                }
            }
        }

        [Fact]
        public async Task Can_append_stream_with_correct_expected_version()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    await store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_correct_expected_version_second_time_with_same_messages_then_should_not_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6));

                    var exception = await Record.ExceptionAsync(() => 
                        store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6)));

                    exception.ShouldBeNull();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_correct_expected_version_second_time_with_same_initial_messages_then_should_not_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6));

                    var exception = await Record.ExceptionAsync(() =>
                        store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5)));

                    exception.ShouldBeNull();
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_correct_expected_version_second_time_with_additional_messages_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6));

                    var exception = await Record.ExceptionAsync(() =>
                        store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6, 7)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamId, 2));
                }
            }
        }

        [Fact]
        public async Task Can_append_to_non_existing_stream_with_expected_version_any()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    var exception = await Record.ExceptionAsync(() => 
                        store.AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3)));
                    exception.ShouldBeNull();

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 4);
                    page.Messages.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task When_append_stream_second_time_with_expected_version_any_and_all_messages_comitted_then_should_be_idempotent()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));

                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 10);
                    page.Messages.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_expected_version_any_and_some_of_the_messages_previously_comitted_then_should_be_idempotent()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));

                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2));

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 10);
                    page.Messages.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task Can_append_stream_with_expected_version_any_and_none_of_then_messages_previously_comitted()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));

                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(4, 5, 6));

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 10);
                    page.Messages.Length.ShouldBe(6);
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_expected_version_any_and_some_of_the_messages_previously_comitted_and_with_additional_messages_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));

                    var exception = await Record.ExceptionAsync(() =>
                        store.AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(2, 3, 4)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamId, ExpectedVersion.Any));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_expected_version_and_duplicate_message_Id_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var exception = await Record.ExceptionAsync(() =>
                        store.AppendToStream(streamId, 2, CreateNewStreamMessages(1)));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamId, 2));
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_empty_collection_of_messages_then_should_create_empty_stream()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store.AppendToStream(streamId, ExpectedVersion.NoStream, new NewStreamMessage[0]);

                    var page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 1);

                    page.Status.ShouldBe(PageReadStatus.Success);
                    page.FromStreamVersion.ShouldBe(0);
                    page.LastStreamVersion.ShouldBe(-1);
                    page.NextStreamVersion.ShouldBe(-1);
                    page.IsEnd.ShouldBe(true);
                }
            }
        }
    }
}
