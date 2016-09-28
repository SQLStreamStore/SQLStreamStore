namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Theory]
        [MemberData("GetReadStreamForwardsTheories")]
        public async Task Can_read_streams_forwards(ReadStreamTheory theory)
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamMessages(4, 5, 6));

                    var streamMessagesPage =
                        await store.ReadStreamForwards(theory.StreamId, theory.Start, theory.PageSize);

                    var expectedStreamMessagesPage = theory.ExpectedStreamMessagesPage;
                    var expectedMessages = theory.ExpectedStreamMessagesPage.Messages.ToArray();

                    streamMessagesPage.FromStreamVersion.ShouldBe(expectedStreamMessagesPage.FromStreamVersion);
                    streamMessagesPage.LastStreamVersion.ShouldBe(expectedStreamMessagesPage.LastStreamVersion);
                    streamMessagesPage.NextStreamVersion.ShouldBe(expectedStreamMessagesPage.NextStreamVersion);
                    streamMessagesPage.ReadDirection.ShouldBe(expectedStreamMessagesPage.ReadDirection);
                    streamMessagesPage.IsEndOfStream.ShouldBe(expectedStreamMessagesPage.IsEndOfStream);
                    streamMessagesPage.Status.ShouldBe(expectedStreamMessagesPage.Status);
                    streamMessagesPage.StreamId.ShouldBe(expectedStreamMessagesPage.StreamId);
                    streamMessagesPage.Messages.Length.ShouldBe(expectedStreamMessagesPage.Messages.Length);

                    for (int i = 0; i < streamMessagesPage.Messages.Length; i++)
                    {
                        var message = streamMessagesPage.Messages.ToArray()[i];
                        var expectedMessage = expectedMessages[i];

                        message.MessageId.ShouldBe(expectedMessage.MessageId);
                        message.JsonData.ShouldBe(expectedMessage.JsonData);
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
        public async Task Can_read_next_page_past_end_of_stream()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadStreamForwards("stream-1", StreamVersion.Start, 4);

                    page = await store.ReadStreamForwards("stream-1", page.NextStreamVersion, 4);

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
        public async Task Can_read_streams_backwards(ReadStreamTheory theory)
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamMessages(4, 5, 6));

                    var streamMessagesPage =
                        await store.ReadStreamBackwards(theory.StreamId, theory.Start, theory.PageSize);

                    var expectedStreamMessagesPage = theory.ExpectedStreamMessagesPage;
                    var expectedMessages = theory.ExpectedStreamMessagesPage.Messages.ToArray();

                    streamMessagesPage.FromStreamVersion.ShouldBe(expectedStreamMessagesPage.FromStreamVersion);
                    streamMessagesPage.LastStreamVersion.ShouldBe(expectedStreamMessagesPage.LastStreamVersion);
                    streamMessagesPage.NextStreamVersion.ShouldBe(expectedStreamMessagesPage.NextStreamVersion);
                    streamMessagesPage.ReadDirection.ShouldBe(expectedStreamMessagesPage.ReadDirection);
                    streamMessagesPage.IsEndOfStream.ShouldBe(expectedStreamMessagesPage.IsEndOfStream);
                    streamMessagesPage.Status.ShouldBe(expectedStreamMessagesPage.Status);
                    streamMessagesPage.StreamId.ShouldBe(expectedStreamMessagesPage.StreamId);
                    streamMessagesPage.Messages.Length.ShouldBe(expectedStreamMessagesPage.Messages.Length);

                    for (int i = 0; i < streamMessagesPage.Messages.Length; i++)
                    {
                        var streamMessage = streamMessagesPage.Messages.ToArray()[i];
                        var expectedMessage = expectedMessages[i];

                        streamMessage.MessageId.ShouldBe(expectedMessage.MessageId);
                        streamMessage.JsonData.ShouldBe(expectedMessage.JsonData);
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
                    new StreamMessagesPage("stream-1", PageReadStatus.Success, 0, 2, 2, ReadDirection.Forward, false,
                          ExpectedStreamMessage("stream-1", 1, 0, SystemClock.GetUtcNow()),
                          ExpectedStreamMessage("stream-1", 2, 1, SystemClock.GetUtcNow()))),

                new ReadStreamTheory("not-exist", 1, 2,
                    new StreamMessagesPage("not-exist", PageReadStatus.StreamNotFound, 1, -1, -1, ReadDirection.Forward, true)),

                new ReadStreamTheory("stream-2", 1, 2,
                    new StreamMessagesPage("stream-2", PageReadStatus.Success, 1, 3, 2, ReadDirection.Forward, true,
                          ExpectedStreamMessage("stream-2", 5, 1, SystemClock.GetUtcNow()),
                          ExpectedStreamMessage("stream-2", 6, 2, SystemClock.GetUtcNow())))
            };

            return theories.Select(t => new object[] { t });
        }

        public static IEnumerable<object[]> GetReadStreamBackwardsTheories()
        {
            var theories = new[]
            {
               new ReadStreamTheory("stream-1", StreamVersion.End, 2,
                    new StreamMessagesPage("stream-1", PageReadStatus.Success, -1, 0, 2, ReadDirection.Backward, false,
                          ExpectedStreamMessage("stream-1", 3, 2, SystemClock.GetUtcNow()),
                          ExpectedStreamMessage("stream-1", 2, 1, SystemClock.GetUtcNow()))),

                 new ReadStreamTheory("stream-1", StreamVersion.End, 4,
                    new StreamMessagesPage("stream-1", PageReadStatus.Success, -1, -1, 2, ReadDirection.Backward, true,
                          ExpectedStreamMessage("stream-1", 3, 2, SystemClock.GetUtcNow()),
                          ExpectedStreamMessage("stream-1", 2, 1, SystemClock.GetUtcNow()),
                          ExpectedStreamMessage("stream-1", 1, 0, SystemClock.GetUtcNow())))
            };

            return theories.Select(t => new object[] { t });
        }

        public class ReadStreamTheory
        {
            public readonly string StreamId;
            public readonly int Start;
            public readonly int PageSize;
            public readonly StreamMessagesPage ExpectedStreamMessagesPage;

            public ReadStreamTheory(
                string streamId,
                int start,
                int pageSize,
                StreamMessagesPage expectedStreamMessagesPage)
            {
                StreamId = streamId;
                Start = start;
                PageSize = pageSize;
                ExpectedStreamMessagesPage = expectedStreamMessagesPage;
            }
        }
    }
}
