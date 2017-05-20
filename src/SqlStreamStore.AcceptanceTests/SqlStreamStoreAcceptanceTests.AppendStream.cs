﻿namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public abstract partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_and_different_message_then_should_throw
            ()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
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
        public async Task
            When_append_stream_second_time_with_no_stream_expected_and_same_messages_then_should_then_should_be_idempotent
            ()
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
        public async Task
            When_append_stream_second_time_with_no_stream_expected_and_same_messages_then_should_then_should_have_expected_result
            ()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));

                    var result = await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));

                    result.CurrentVersion.ShouldBe(1);
                    result.CurrentPosition.ShouldBe(1L);
                    //result.NextExpectedVersion.ShouldBe(1);
                }
            }
        }

        [Fact]
        public async Task
            When_append_stream_second_time_with_no_stream_expected_and_additional_messages_then_should_throw()
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
        public async Task
            When_append_stream_second_time_with_no_stream_expected_and_same_inital_message_then_should_be_idempotent()
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
        public async Task
            When_append_stream_second_time_with_no_stream_expected_and_same_inital_message_then_should_have_expected_result()
        {
            // Idempotency
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2));

                    var result =
                        await store.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1));

                    result.CurrentVersion.ShouldBe(1);
                    result.CurrentPosition.ShouldBe(1L);
                    //result.NextExpectedVersion.ShouldBe(0);
                }
            }
        }

        [Fact]
        public async Task
            When_append_stream_second_time_with_no_stream_expected_and_different_inital_messages_then_should_throw()
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
        public async Task Can_append_multiple_messages_to_stream_with_correct_expected_version()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    var result = await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    result =
                        await store.AppendToStream(streamId, result.CurrentVersion, CreateNewStreamMessages(4, 5, 6));

                    result.CurrentVersion.ShouldBe(5);
                    result.CurrentPosition.ShouldBe(5L);
                }
            }
        }

        [Fact]
        public async Task Can_append_single_message_to_stream_with_correct_expected_version()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    var result = await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    result =
                        await store.AppendToStream(streamId, result.CurrentVersion, CreateNewStreamMessages(4)[0]);

                    result.CurrentVersion.ShouldBe(3);
                    result.CurrentPosition.ShouldBe(3L);
                }
            }
        }

        [Fact]
        public async Task
            When_append_stream_with_correct_expected_version_second_time_with_same_messages_then_should_not_throw()
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
        public async Task When_append_multiple_messages_to_stream_with_correct_expected_version_second_time_with_same_messages_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6));

                    var result = await
                            store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6));

                    result.CurrentVersion.ShouldBe(5);
                    result.CurrentPosition.ShouldBe(5L);
                    //result.NextExpectedVersion.ShouldBe(5);
                }
            }
        }

        [Fact]
        public async Task When_append_single_message_to_stream_with_correct_expected_version_second_time_with_same_messages_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream(streamId, 2, CreateNewStreamMessages(4)[0]);

                    var result = await
                            store.AppendToStream(streamId, 2, CreateNewStreamMessages(4)[0]);

                    result.CurrentVersion.ShouldBe(3);
                    result.CurrentPosition.ShouldBe(3L);
                    //result.NextExpectedVersion.ShouldBe(4);
                }
            }
        }

        [Fact]
        public async Task
            When_append_stream_with_correct_expected_version_second_time_with_same_initial_messages_then_should_not_throw
            ()
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
        public async Task When_append_multiple_messages_to_stream_with_correct_expected_version_second_time_with_same_initial_messages_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5, 6));

                    var result = await
                            store.AppendToStream(streamId, 2, CreateNewStreamMessages(4, 5));

                    result.CurrentVersion.ShouldBe(5);
                    result.CurrentPosition.ShouldBe(5L);
                    //result.NextExpectedVersion.ShouldBe(5);
                }
            }
        }

        [Fact]
        public async Task When_append_single_message_to_stream_with_correct_expected_version_second_time_with_same_initial_messages_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream(streamId, 2, CreateNewStreamMessages(4)[0]);

                    var result = await
                            store.AppendToStream(streamId, 1, CreateNewStreamMessages(3)[0]);

                    result.CurrentVersion.ShouldBe(3);
                    result.CurrentPosition.ShouldBe(3L);
                    //result.NextExpectedVersion.ShouldBe(4);
                }
            }
        }

        [Fact]
        public async Task
            When_append_stream_with_correct_expected_version_second_time_with_additional_messages_then_should_throw()
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
        public async Task Can_append_multiple_messages_to_non_existing_stream_with_expected_version_any()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    var result =
                        await store.AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));

                    result.CurrentVersion.ShouldBe(2);

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 4);
                    page.Messages.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task Can_append_single_message_to_non_existing_stream_with_expected_version_any()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    var result =
                        await store.AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1)[0]);

                    result.CurrentVersion.ShouldBe(0);
                    result.CurrentPosition.ShouldBe(0L);

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 2);
                    page.Messages.Length.ShouldBe(1);
                }
            }
        }

        [Fact]
        public async Task
            When_append_stream_second_time_with_expected_version_any_and_all_messages_committed_then_should_be_idempotent
            ()
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
        public async Task When_append_multiple_messages_to_stream_second_time_with_expected_version_any_and_all_messages_committed_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    var result1 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));

                    result1.CurrentVersion.ShouldBe(2);
                    result1.CurrentPosition.ShouldBe(2L);
                    //result1.NextExpectedVersion.ShouldBe(2);

                    var result2 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));

                    result2.CurrentVersion.ShouldBe(2);
                    result2.CurrentPosition.ShouldBe(2L);
                    //result2.NextExpectedVersion.ShouldBe(2);

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 10);
                    page.Messages.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task When_append_single_message_to_stream_second_time_with_expected_version_any_and_all_messages_committed_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    var result1 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1)[0]);

                    result1.CurrentVersion.ShouldBe(0);
                    result1.CurrentPosition.ShouldBe(0L);
                    //result1.NextExpectedVersion.ShouldBe(0);

                    var result2 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1)[0]);

                    result2.CurrentVersion.ShouldBe(0);
                    result2.CurrentPosition.ShouldBe(0L);
                    //result2.NextExpectedVersion.ShouldBe(0);

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 3);
                    page.Messages.Length.ShouldBe(1);
                }
            }
        }

        [Fact]
        public async Task
            When_append_stream_with_expected_version_any_and_some_of_the_messages_previously_committed_then_should_be_idempotent
            ()
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
        public async Task When_append_multiple_messages_to_stream_with_expected_version_any_and_some_of_the_messages_previously_committed_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    var result1 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));
                    result1.CurrentVersion.ShouldBe(2);
                    result1.CurrentPosition.ShouldBe(2L);
                    //result1.NextExpectedVersion.ShouldBe(2);

                    var result2 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2));

                    result2.CurrentVersion.ShouldBe(2);
                    result2.CurrentPosition.ShouldBe(2L);
                    //result1.NextExpectedVersion.ShouldBe(1);

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 10);
                    page.Messages.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task When_append_single_message_to_stream_with_expected_version_any_and_some_of_the_messages_previously_committed_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    var result1 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));
                    result1.CurrentVersion.ShouldBe(2);
                    result1.CurrentPosition.ShouldBe(2L);
                    //result1.NextExpectedVersion.ShouldBe(2);

                    var result2 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1)[0]);

                    result2.CurrentVersion.ShouldBe(2);
                    result2.CurrentPosition.ShouldBe(2L);
                    //result1.NextExpectedVersion.ShouldBe(1);

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 4);
                    page.Messages.Length.ShouldBe(3);
                }
            }
        }

        [Fact]
        public async Task Can_append_stream_with_expected_version_any_and_none_of_the_messages_previously_committed()
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
        public async Task Can_append_multiple_messages_to_stream_with_expected_version_any_and_none_of_the_messages_previously_committed_should_have_expected_results()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    var result1 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));
                    result1.CurrentVersion.ShouldBe(2);
                    result1.CurrentPosition.ShouldBe(2L);

                    var result2 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(4, 5, 6));
                    result2.CurrentVersion.ShouldBe(5);
                    result2.CurrentPosition.ShouldBe(5L);

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 10);
                    page.Messages.Length.ShouldBe(6);
                }
            }
        }


        [Fact]
        public async Task Can_append_single_message_to_stream_with_expected_version_any_and_none_of_the_messages_previously_committed_should_have_expected_results()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";

                    var result1 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(1, 2, 3));
                    result1.CurrentVersion.ShouldBe(2);
                    result1.CurrentPosition.ShouldBe(2L);

                    var result2 = await store
                        .AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamMessages(4)[0]);
                    result2.CurrentVersion.ShouldBe(3);
                    result2.CurrentPosition.ShouldBe(3L);

                    var page = await store
                        .ReadStreamForwards(streamId, StreamVersion.Start, 5);
                    page.Messages.Length.ShouldBe(4);
                }
            }
        }
        [Fact]
        public async Task
            When_append_stream_with_expected_version_any_and_some_of_the_messages_previously_committed_and_with_additional_messages_then_should_throw
            ()
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
        public async Task When_append_stream_with_expected_version_and_no_messages_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var result = await store.AppendToStream(streamId, 2, new NewStreamMessage[0]);

                    result.CurrentVersion.ShouldBe(2);
                    result.CurrentPosition.ShouldBe(2);
                }
            }
        }

        [Fact]
        public async Task When_append_stream_with_expected_version_no_stream_and_no_messages_then_should_have_expected_result()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    var result = await store
                        .AppendToStream(streamId, ExpectedVersion.NoStream, new NewStreamMessage[0]);

                    result.CurrentVersion.ShouldBe(-1);
                    result.CurrentPosition.ShouldBe(-1);
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

        [Theory]
        [InlineData(ExpectedVersion.NoStream)]
        [InlineData(ExpectedVersion.Any)]
        public async Task When_append_to_non_existent_stream_with_empty_collection_of_messages_then_should_create_empty_stream(int expectedVersion)
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId = "stream-1";
                    await store.AppendToStream(streamId, expectedVersion, new NewStreamMessage[0]);

                    var page = await store.ReadStreamForwards(streamId, StreamVersion.Start, 1);

                    page.Status.ShouldBe(PageReadStatus.Success);
                    page.FromStreamVersion.ShouldBe(0);
                    page.LastStreamVersion.ShouldBe(-1);
                    page.LastStreamPosition.ShouldBe(-1);
                    page.NextStreamVersion.ShouldBe(0);
                    page.IsEnd.ShouldBe(true);
                }
            }
        }

        [Theory]
        [InlineData(ExpectedVersion.Any)]
        [InlineData(ExpectedVersion.NoStream)]
        public async Task When_append_to_many_streams_returns_expected_position(int expectedVersion)
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    const string streamId1 = "stream-1";
                    const string streamId2 = "stream-2";

                    var result1 =
                        await store.AppendToStream(streamId1, expectedVersion, CreateNewStreamMessages(1, 2, 3));

                    result1.CurrentVersion.ShouldBe(2);
                    result1.CurrentPosition.ShouldBe(2L);

                    var result2 =
                        await store.AppendToStream(streamId2, expectedVersion, CreateNewStreamMessages(1, 2, 3));

                    result2.CurrentVersion.ShouldBe(2);
                    result2.CurrentPosition.ShouldBe(5L);
                }
            }
        }
    }
}
