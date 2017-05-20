namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using Xunit;
    using static SqlStreamStore.Infrastructure.StreamStoreConstants;

    public abstract partial class StreamStoreAcceptanceTests
    {
        [Theory]
        [InlineData(-1)]
        [InlineData(-2)]
        [InlineData(long.MinValue)]
        public async Task ReadAllForwards_Throws_An_ArgumentException_When_Position_Is_Less_Than_Zero(long invalidPosition)
        {
            // given
            var pageSize = MinimumPageSize;

            // when
            var exception = await Record.ExceptionAsync(() => store.ReadAllForwards(invalidPosition, pageSize));

            // then
            exception.ShouldBeOfType<ArgumentException>();
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(-2)]
        [InlineData(int.MinValue)]
        public async Task ReadAllForwards_Throws_An_ArgumentException_When_PageSize_Is_Less_Than_One(int invalidPageSize)
        {
            // given
            var position = MinimumReadAllForwardPosition;

            // when
            var exception = await Record.ExceptionAsync(() => store.ReadAllForwards(position, invalidPageSize));

            // then
            exception.ShouldBeOfType<ArgumentException>();
        }

        [Fact]
        public async Task ReadAllForwards_Throws_An_ObjectDisposedException_When_The_StreamStore_Has_Been_Disposed()
        {
            // given
            var position = MinimumReadAllForwardPosition;
            var pageSize = MinimumPageSize;
            store.Dispose();

            // when
            var exception = await Record.ExceptionAsync(() => store.ReadAllForwards(position, pageSize));

            // then
            exception.ShouldBeOfType<ObjectDisposedException>();
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(long.MaxValue)]
        public async Task ReadAllForwards_Returns_The_Expected_Page_When_The_Store_Is_Empty(long position)
        {
            // when
            var page = await store.ReadAllForwards(position, int.MaxValue);

            // then
            page.FromPosition.ShouldBe(position, $"FromPosition should be: {position}");
            page.NextPosition.ShouldBe(0, $"NextPosition");
            page.IsEnd.ShouldBeTrue();
            page.Direction.ShouldBe(ReadDirection.Forward);
            page.Messages.ShouldBeEmpty();
        }

        [Fact]
        public async Task Can_read_all_forwards()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamMessages(4, 5, 6));
                    var expectedMessages = new[]
                    {
                        ExpectedStreamMessage("stream-1", 1, 0, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-1", 2, 1, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-1", 3, 2, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-2", 4, 0, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-2", 5, 1, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-2", 6, 2, fixture.GetUtcNow())
                    };

                    var page = await store.ReadAllForwards(Position.Start, 4);
                    List<StreamMessage> messages = new List<StreamMessage>(page.Messages);
                    int count = 0;
                    while(!page.IsEnd && count <20) //should not take more than 20 iterations.
                    {
                        page = await page.ReadNext();
                        messages.AddRange(page.Messages);
                        count++;
                    }

                    count.ShouldBeLessThan(20);
                    page.Direction.ShouldBe(ReadDirection.Forward);
                    page.IsEnd.ShouldBeTrue();

                    for (int i = 0; i < messages.Count; i++)
                    {
                        var message = messages[i];
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
        public async Task Can_read_all_forwards_without_prefetch()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadAllForwards(Position.Start, 4, prefetchJsonData: false);

                    foreach(var streamMessage in page.Messages)
                    {
                        streamMessage.GetJsonData().IsCompleted.ShouldBeFalse();

                        (await streamMessage.GetJsonData()).ShouldNotBeNullOrWhiteSpace();
                    }
                }
            }
        }

        [Fact]
        public async Task When_read_without_prefetch_and_stream_is_deleted_then_GetJsonData_should_return_null()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadAllForwards(Position.Start, 4, prefetchJsonData: false);

                    await store.DeleteStream("stream-1");

                    foreach (var streamMessage in page.Messages)
                    {
                        streamMessage.GetJsonData().IsCompleted.ShouldBeFalse();

                        (await streamMessage.GetJsonData()).ShouldBeNull();
                    }
                }
            }
        }

        [Fact]
        public async Task Can_read_all_backwards()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));
                    await store.AppendToStream("stream-2", ExpectedVersion.NoStream, CreateNewStreamMessages(4, 5, 6));
                    var expectedMessages = new[]
                    {
                        ExpectedStreamMessage("stream-1", 1, 0, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-1", 2, 1, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-1", 3, 2, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-2", 4, 0, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-2", 5, 1, fixture.GetUtcNow()),
                        ExpectedStreamMessage("stream-2", 6, 2, fixture.GetUtcNow())
                    }.Reverse().ToArray();

                    var page = await store.ReadAllBackwards(Position.End, 4);
                    List<StreamMessage> messages = new List<StreamMessage>(page.Messages);
                    int count = 0;
                    while (!page.IsEnd && count < 20) //should not take more than 20 iterations.
                    {
                        page = await store.ReadAllBackwards(page.NextPosition, 10);
                        messages.AddRange(page.Messages);
                        count++;
                    }

                    count.ShouldBeLessThan(20);
                    page.Direction.ShouldBe(ReadDirection.Backward);
                    page.IsEnd.ShouldBeTrue();

                    messages.Count.ShouldBe(expectedMessages.Length);

                    for(int i = 0; i < messages.Count; i++)
                    {
                        var message = messages[i];
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
        public async Task Can_read_all_backwards_without_prefetch()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadAllBackwards(Position.Start, 4, prefetchJsonData: false);

                    foreach (var streamMessage in page.Messages)
                    {
                        streamMessage.GetJsonData().IsCompleted.ShouldBeFalse();

                        (await streamMessage.GetJsonData()).ShouldNotBeNullOrWhiteSpace();
                    }
                }
            }
        }

        [Theory]
        [InlineData(3, 0, 3, 3, 0, 3)]  // Read entire store
        [InlineData(3, 0, 4, 3, 0, 3)]  // Read entire store
        [InlineData(3, 0, 2, 2, 0, 2)]
        [InlineData(3, 1, 2, 2, 1, 3)]
        [InlineData(3, 2, 1, 1, 2, 3)]
        [InlineData(3, 3, 1, 0, 3, 3)]
        public async Task When_read_all_forwards(
            int numberOfSeedMessages,
            int fromPosition,
            int maxCount,
            int expectedCount,
            int expectedFromPosition,
            int expectedNextCheckPoint)
        {
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream(
                        "stream-1",
                        ExpectedVersion.NoStream,
                        CreateNewStreamMessageSequence(1, numberOfSeedMessages));

                    var page = await store.ReadAllForwards(fromPosition, maxCount);

                    page.Messages.Length.ShouldBe(expectedCount);
                    page.FromPosition.ShouldBe(expectedFromPosition);
                    page.NextPosition.ShouldBe(expectedNextCheckPoint);
                }
            }
        }

        [Theory]
        [InlineData(3, -1, 1, 1, 2, 1)] // -1 is Position.End
        [InlineData(3, 2, 1, 1, 2, 1)]
        [InlineData(3, 1, 1, 1, 1, 0)]
        [InlineData(3, 0, 1, 1, 0, 0)]
        [InlineData(3, -1, 3, 3, 2, 0)] // Read entire store
        [InlineData(3, -1, 4, 3, 2, 0)] // Read entire store
        [InlineData(0, -1, 1, 0, 0, 0)]
        public async Task When_read_all_backwards(
            int numberOfSeedMessages,
            int fromPosition,
            int maxCount,
            int expectedCount,
            int expectedFromPosition,
            int expectedNextCheckPoint)
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    if(numberOfSeedMessages > 0)
                    {
                        await store.AppendToStream(
                            "stream-1",
                            ExpectedVersion.NoStream,
                            CreateNewStreamMessageSequence(1, numberOfSeedMessages));
                    }

                    var allMessagesPage = await store.ReadAllBackwards(fromPosition, maxCount);

                    allMessagesPage.Messages.Length.ShouldBe(expectedCount);
                    allMessagesPage.FromPosition.ShouldBe(expectedFromPosition);
                    allMessagesPage.NextPosition.ShouldBe(expectedNextCheckPoint);
                }
            }
        }
    }
}
