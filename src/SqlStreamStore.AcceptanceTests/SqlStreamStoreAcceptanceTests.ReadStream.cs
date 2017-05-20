﻿namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Infrastructure;
    using Xunit;

    public abstract partial class StreamStoreAcceptanceTests
    {
        [Theory]
        [MemberData("GetReadStreamForwardsTheories")]
        public async Task Can_read_streams_forwards_with_prefetch(ReadStreamTheory theory)
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamMessages(4, 5, 6));

                    var streamMessagesPage =
                        await store.ReadStreamForwards(theory.StreamId, theory.Start, theory.PageSize);

                    var expectedStreamMessagesPage = theory.ExpectedReadStreamPage;
                    var expectedMessages = theory.ExpectedReadStreamPage.Messages.ToArray();

                    streamMessagesPage.FromStreamVersion.ShouldBe(expectedStreamMessagesPage.FromStreamVersion);
                    streamMessagesPage.LastStreamVersion.ShouldBe(expectedStreamMessagesPage.LastStreamVersion);
                    streamMessagesPage.NextStreamVersion.ShouldBe(expectedStreamMessagesPage.NextStreamVersion);
                    streamMessagesPage.ReadDirection.ShouldBe(expectedStreamMessagesPage.ReadDirection);
                    streamMessagesPage.IsEnd.ShouldBe(expectedStreamMessagesPage.IsEnd);
                    streamMessagesPage.Status.ShouldBe(expectedStreamMessagesPage.Status);
                    streamMessagesPage.StreamId.ShouldBe(expectedStreamMessagesPage.StreamId);
                    streamMessagesPage.Messages.Length.ShouldBe(expectedStreamMessagesPage.Messages.Length);

                    for (int i = 0; i < streamMessagesPage.Messages.Length; i++)
                    {
                        var message = streamMessagesPage.Messages.ToArray()[i];
                        var expectedMessage = expectedMessages[i];

                        message.MessageId.ShouldBe(expectedMessage.MessageId);
                        (await message.GetJsonData()).ShouldBe(await expectedMessage.GetJsonData());
                        message.JsonMetadata.ShouldBe(expectedMessage.JsonMetadata);
                        message.StreamId.ShouldBe(expectedMessage.StreamId);
                        message.StreamVersion.ShouldBe(expectedMessage.StreamVersion);
                        message.Type.ShouldBe(expectedMessage.Type);

                        // We don't care about StreamMessage.Position and StreamMessage.Position
                        // as they are non-deterministic
                    }
                }
            }
        }

        [Theory]
        [MemberData("GetReadStreamForwardsTheories")]
        public async Task Can_read_streams_forwards_without_prefetch(ReadStreamTheory theory)
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamMessages(4, 5, 6));

                    var streamMessagesPage =
                        await store.ReadStreamForwards(theory.StreamId, theory.Start, theory.PageSize, false);

                    var expectedStreamMessagesPage = theory.ExpectedReadStreamPage;
                    var expectedMessages = theory.ExpectedReadStreamPage.Messages.ToArray();

                    streamMessagesPage.FromStreamVersion.ShouldBe(expectedStreamMessagesPage.FromStreamVersion);
                    streamMessagesPage.LastStreamVersion.ShouldBe(expectedStreamMessagesPage.LastStreamVersion);
                    streamMessagesPage.NextStreamVersion.ShouldBe(expectedStreamMessagesPage.NextStreamVersion);
                    streamMessagesPage.ReadDirection.ShouldBe(expectedStreamMessagesPage.ReadDirection);
                    streamMessagesPage.IsEnd.ShouldBe(expectedStreamMessagesPage.IsEnd);
                    streamMessagesPage.Status.ShouldBe(expectedStreamMessagesPage.Status);
                    streamMessagesPage.StreamId.ShouldBe(expectedStreamMessagesPage.StreamId);
                    streamMessagesPage.Messages.Length.ShouldBe(expectedStreamMessagesPage.Messages.Length);

                    for (int i = 0; i < streamMessagesPage.Messages.Length; i++)
                    {
                        var message = streamMessagesPage.Messages.ToArray()[i];
                        var expectedMessage = expectedMessages[i];

                        message.MessageId.ShouldBe(expectedMessage.MessageId);
                        (await message.GetJsonData()).ShouldBe(await expectedMessage.GetJsonData());
                        message.JsonMetadata.ShouldBe(expectedMessage.JsonMetadata);
                        message.StreamId.ShouldBe(expectedMessage.StreamId);
                        message.StreamVersion.ShouldBe(expectedMessage.StreamVersion);
                        message.Type.ShouldBe(expectedMessage.Type);

                        // We don't care about StreamMessage.Position and StreamMessage.Position
                        // as they are non-deterministic
                    }
                }
            }
        }

        [Fact]
        public async Task Can_read_whole_stream_forwards_without_prefetch()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadStreamForwards("stream-1", StreamVersion.Start, 5, prefetchJsonData: false);

                    foreach (var streamMessage in page.Messages)
                    {
                        streamMessage.GetJsonData().IsCompleted.ShouldBeFalse();

                        (await streamMessage.GetJsonData()).ShouldNotBeNullOrWhiteSpace();
                    }
                }
            }
        }

        [Fact]
        public async Task Can_read_next_page_past_end_of_stream()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadStreamForwards("stream-1", StreamVersion.Start, 4);

                    page = await page.ReadNext();

                    page.Messages.Length.ShouldBe(0);
                }
            }
        }

        [Fact]
        public async Task Can_read_all_messages()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadStreamForwards("stream-1", StreamVersion.Start, int.MaxValue);

                    page.Messages.Length.ShouldBe(3);
                }
            }
        }

        [Theory]
        [MemberData("GetReadStreamBackwardsTheories")]
        public async Task Can_read_streams_backwards_with_prefetch(ReadStreamTheory theory)
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamMessages(4, 5, 6));

                    var streamMessagesPage =
                        await store.ReadStreamBackwards(theory.StreamId, theory.Start, theory.PageSize);

                    var expectedStreamMessagesPage = theory.ExpectedReadStreamPage;
                    var expectedMessages = theory.ExpectedReadStreamPage.Messages.ToArray();

                    streamMessagesPage.FromStreamVersion.ShouldBe(expectedStreamMessagesPage.FromStreamVersion);
                    streamMessagesPage.LastStreamVersion.ShouldBe(expectedStreamMessagesPage.LastStreamVersion);
                    streamMessagesPage.NextStreamVersion.ShouldBe(expectedStreamMessagesPage.NextStreamVersion);
                    streamMessagesPage.ReadDirection.ShouldBe(expectedStreamMessagesPage.ReadDirection);
                    streamMessagesPage.IsEnd.ShouldBe(expectedStreamMessagesPage.IsEnd);
                    streamMessagesPage.Status.ShouldBe(expectedStreamMessagesPage.Status);
                    streamMessagesPage.StreamId.ShouldBe(expectedStreamMessagesPage.StreamId);
                    streamMessagesPage.Messages.Length.ShouldBe(expectedStreamMessagesPage.Messages.Length);

                    for (int i = 0; i < streamMessagesPage.Messages.Length; i++)
                    {
                        var streamMessage = streamMessagesPage.Messages.ToArray()[i];
                        var expectedMessage = expectedMessages[i];

                        streamMessage.MessageId.ShouldBe(expectedMessage.MessageId);
                        (await streamMessage.GetJsonData()).ShouldBe(await expectedMessage.GetJsonData());
                        streamMessage.JsonMetadata.ShouldBe(expectedMessage.JsonMetadata);
                        streamMessage.StreamId.ShouldBe(expectedMessage.StreamId);
                        streamMessage.StreamVersion.ShouldBe(expectedMessage.StreamVersion);
                        streamMessage.Type.ShouldBe(expectedMessage.Type);

                        // We don't care about StreamMessage.Position and StreamMessage.Position
                        // as they are non-deterministic
                    }
                }
            }
        }

        [Theory]
        [MemberData("GetReadStreamBackwardsTheories")]
        public async Task Can_read_streams_backwards_without_prefetch(ReadStreamTheory theory)
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamMessages(4, 5, 6));

                    var streamMessagesPage =
                        await store.ReadStreamBackwards(theory.StreamId, theory.Start, theory.PageSize, false);

                    var expectedStreamMessagesPage = theory.ExpectedReadStreamPage;
                    var expectedMessages = theory.ExpectedReadStreamPage.Messages.ToArray();

                    streamMessagesPage.FromStreamVersion.ShouldBe(expectedStreamMessagesPage.FromStreamVersion);
                    streamMessagesPage.LastStreamVersion.ShouldBe(expectedStreamMessagesPage.LastStreamVersion);
                    streamMessagesPage.NextStreamVersion.ShouldBe(expectedStreamMessagesPage.NextStreamVersion);
                    streamMessagesPage.ReadDirection.ShouldBe(expectedStreamMessagesPage.ReadDirection);
                    streamMessagesPage.IsEnd.ShouldBe(expectedStreamMessagesPage.IsEnd);
                    streamMessagesPage.Status.ShouldBe(expectedStreamMessagesPage.Status);
                    streamMessagesPage.StreamId.ShouldBe(expectedStreamMessagesPage.StreamId);
                    streamMessagesPage.Messages.Length.ShouldBe(expectedStreamMessagesPage.Messages.Length);

                    for (int i = 0; i < streamMessagesPage.Messages.Length; i++)
                    {
                        var streamMessage = streamMessagesPage.Messages.ToArray()[i];
                        var expectedMessage = expectedMessages[i];

                        streamMessage.MessageId.ShouldBe(expectedMessage.MessageId);
                        (await streamMessage.GetJsonData()).ShouldBe(await expectedMessage.GetJsonData());
                        streamMessage.JsonMetadata.ShouldBe(expectedMessage.JsonMetadata);
                        streamMessage.StreamId.ShouldBe(expectedMessage.StreamId);
                        streamMessage.StreamVersion.ShouldBe(expectedMessage.StreamVersion);
                        streamMessage.Type.ShouldBe(expectedMessage.Type);

                        // We don't care about StreamMessage.Position and StreamMessage.Position
                        // as they are non-deterministic
                    }
                }
            }
        }

        [Fact]
        public async Task Can_read_stream_backwards_without_prefetch()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadStreamBackwards("stream-1", StreamVersion.End, 5, prefetchJsonData: false);

                    foreach (var streamMessage in page.Messages)
                    {
                        streamMessage.GetJsonData().IsCompleted.ShouldBeFalse();

                        (await streamMessage.GetJsonData()).ShouldNotBeNullOrWhiteSpace();
                    }
                }
            }
        }

        [Fact]
        public async Task Can_read_empty_stream_backwards()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages());

                    var page = await store.ReadStreamBackwards("stream-1", StreamVersion.End, 1);
                    page.Status.ShouldBe(PageReadStatus.Success);
                    page.Messages.Length.ShouldBe(0);
                    page.FromStreamVersion.ShouldBe(StreamVersion.End);
                    page.IsEnd.ShouldBeTrue();
                    page.LastStreamVersion.ShouldBe(StreamVersion.End);
                    page.LastStreamPosition.ShouldBe(-1);
                    page.NextStreamVersion.ShouldBe(StreamVersion.End);
                    page.ReadDirection.ShouldBe(ReadDirection.Backward);
                    page.StreamId.ShouldBe("stream-1");
                }
            }
        }

        [Fact]
        public async Task Can_read_empty_stream_forwards()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages());

                    var page = await store.ReadStreamForwards("stream-1", StreamVersion.Start, 1);
                    page.Status.ShouldBe(PageReadStatus.Success);
                    page.Messages.Length.ShouldBe(0);
                    page.FromStreamVersion.ShouldBe(StreamVersion.Start);
                    page.IsEnd.ShouldBeTrue();
                    page.LastStreamVersion.ShouldBe(StreamVersion.End);
                    page.LastStreamPosition.ShouldBe(-1);
                    page.NextStreamVersion.ShouldBe(StreamVersion.Start);
                    page.ReadDirection.ShouldBe(ReadDirection.Forward);
                    page.StreamId.ShouldBe("stream-1");
                }
            }
        }

        [Fact]
        public async Task When_read_non_exist_stream_forwards_then_should_get_StreamNotFound()
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    var streamMessagesPage =
                        await store.ReadStreamForwards("stream-does-not-exist", StreamVersion.Start, 1);

                    streamMessagesPage.Status.ShouldBe(PageReadStatus.StreamNotFound);
                }
            }
        }

        [Fact]
        public async Task When_read_non_exist_stream_backwards_then_should_get_StreamNotFound()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    var streamMessagesPage =
                        await store.ReadStreamBackwards("stream-does-not-exist", StreamVersion.End, 1);

                    streamMessagesPage.Status.ShouldBe(PageReadStatus.StreamNotFound);
                }
            }
        }

        [Fact]
        public async Task When_read_deleted_stream_forwards_then_should_get_StreamNotFound()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.DeleteStream("stream-1");

                    var streamMessagesPage =
                        await store.ReadStreamForwards("stream-1", StreamVersion.Start, 1);

                    streamMessagesPage.Status.ShouldBe(PageReadStatus.StreamNotFound);
                }
            }
        }

        [Fact]
        public async Task When_read_deleted_stream_backwards_then_should_get_StreamNotFound()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.DeleteStream("stream-1");

                    var streamMessagesPage =
                        await store.ReadStreamBackwards("stream-1", StreamVersion.Start, 1);

                    streamMessagesPage.Status.ShouldBe(PageReadStatus.StreamNotFound);
                }
            }
        }

        // ReSharper disable once UnusedMethodReturnValue.Global
        public static IEnumerable<object[]> GetReadStreamForwardsTheories()
        {
            var theories = new[]
            {
                new ReadStreamTheory("stream-1", StreamVersion.Start, 2,
                    new ReadStreamPage("stream-1", PageReadStatus.Success, 0, 2, 2, -1, ReadDirection.Forward, false,
                    new [] {
                        ExpectedStreamMessage("stream-1", 1, 0, SystemClock.GetUtcNow()),
                        ExpectedStreamMessage("stream-1", 2, 1, SystemClock.GetUtcNow())
                    })),

                new ReadStreamTheory("not-exist", 1, 2,
                    new ReadStreamPage("not-exist", PageReadStatus.StreamNotFound, 1, -1, -1, -1, ReadDirection.Forward, true,
                        StreamMessage.EmptyArray)),

                new ReadStreamTheory("stream-2", 1, 2,
                    new ReadStreamPage("stream-2", PageReadStatus.Success, 1, 3, 2, -1, ReadDirection.Forward, true,
                    new [] {
                        ExpectedStreamMessage("stream-2", 5, 1, SystemClock.GetUtcNow()),
                        ExpectedStreamMessage("stream-2", 6, 2, SystemClock.GetUtcNow())
                    }))
            };

            return theories.Select(t => new object[] { t });
        }

        public static IEnumerable<object[]> GetReadStreamBackwardsTheories()
        {
            var theories = new[]
            {
                new ReadStreamTheory("stream-1", StreamVersion.End, 1,
                    new ReadStreamPage("stream-1", PageReadStatus.Success, -1, 1, 2, -1, ReadDirection.Backward, false,
                        new [] {
                            ExpectedStreamMessage("stream-1", 3, 2, SystemClock.GetUtcNow())
                        })),

                new ReadStreamTheory("stream-1", StreamVersion.End, 2,
                    new ReadStreamPage("stream-1", PageReadStatus.Success, -1, 0, 2, -1, ReadDirection.Backward, false,
                        new [] {
                            ExpectedStreamMessage("stream-1", 3, 2, SystemClock.GetUtcNow()),
                            ExpectedStreamMessage("stream-1", 2, 1, SystemClock.GetUtcNow())
                        })),

                 new ReadStreamTheory("stream-1", StreamVersion.End, 4,
                    new ReadStreamPage("stream-1", PageReadStatus.Success, -1, -1, 2, -1, ReadDirection.Backward, true,
                        new [] {
                            ExpectedStreamMessage("stream-1", 3, 2, SystemClock.GetUtcNow()),
                            ExpectedStreamMessage("stream-1", 2, 1, SystemClock.GetUtcNow()),
                            ExpectedStreamMessage("stream-1", 1, 0, SystemClock.GetUtcNow())
                        }))
            };

            return theories.Select(t => new object[] { t });
        }

        public class ReadStreamTheory
        {
            public readonly string StreamId;
            public readonly int Start;
            public readonly int PageSize;
            public readonly ReadStreamPage ExpectedReadStreamPage;

            public ReadStreamTheory(
                string streamId,
                int start,
                int pageSize,
                ReadStreamPage expectedReadStreamPage)
            {
                StreamId = streamId;
                Start = start;
                PageSize = pageSize;
                ExpectedReadStreamPage = expectedReadStreamPage;
            }
        }
    }
}
