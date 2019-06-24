namespace SqlStreamStore
{
    using System.Linq;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class AcceptanceTests
    {
        [Fact, Trait("Category", "ReadAll")]
        public async Task Can_read_all_forwards()
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

            var page = store.ReadAllForwards(Position.Start, 4);
            var messages = await page.ToListAsync();

            page.ReadDirection.ShouldBe(ReadDirection.Forward);
            page.IsEnd.ShouldBeTrue();

            for(int i = 0; i < messages.Count; i++)
            {
                var message = messages[i];
                var expectedMessage = expectedMessages[i];

                message.MessageId.ShouldBe(expectedMessage.MessageId);
                var jsonData = await message.GetJsonData();
                var expectedJsonData = await expectedMessage.GetJsonData();
                JToken.DeepEquals(
                        JObject.Parse(jsonData),
                        JObject.Parse(expectedJsonData))
                    .ShouldBeTrue();
                JToken.DeepEquals(JObject.Parse(message.JsonMetadata), JObject.Parse(expectedMessage.JsonMetadata))
                    .ShouldBeTrue();
                message.StreamId.ShouldBe(expectedMessage.StreamId);
                message.StreamVersion.ShouldBe(expectedMessage.StreamVersion);
                message.Type.ShouldBe(expectedMessage.Type);

                // We don't care about StreamMessage.Position and StreamMessage.Position
                // as they are non-deterministic
            }
        }

        /*[Fact, Trait("Category", "ReadAll")]
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
        }*/

        [Fact, Trait("Category", "ReadAll")]
        public async Task When_read_without_prefetch_and_stream_is_deleted_then_GetJsonData_should_return_null()
        {
            await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

            var page = store.ReadAllForwards(Position.Start, 4, prefetchJsonData: false);

            await store.DeleteStream("stream-1");

            await foreach(var streamMessage in page.Where(x => !x.StreamId.StartsWith("$"))
            ) // could be the deleted message
            {
                (await streamMessage.GetJsonData()).ShouldBeNull();
            }
        }

        [Fact, Trait("Category", "ReadAll")]
        public async Task Can_read_all_backwards()
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

            var page = store.ReadAllBackwards(Position.End, 4);
            var messages = await page.ToListAsync();

            page.ReadDirection.ShouldBe(ReadDirection.Backward);
            page.IsEnd.ShouldBeTrue();

            messages.Count.ShouldBe(expectedMessages.Length);

            for(int i = 0; i < messages.Count; i++)
            {
                var message = messages[i];
                var expectedMessage = expectedMessages[i];

                message.MessageId.ShouldBe(expectedMessage.MessageId);
                var jsonData = await message.GetJsonData();
                var expectedJsonData = await expectedMessage.GetJsonData();
                JToken.DeepEquals(
                        JObject.Parse(jsonData),
                        JObject.Parse(expectedJsonData))
                    .ShouldBeTrue();
                JToken.DeepEquals(JObject.Parse(message.JsonMetadata), JObject.Parse(expectedMessage.JsonMetadata))
                    .ShouldBeTrue();
                message.StreamId.ShouldBe(expectedMessage.StreamId);
                message.StreamVersion.ShouldBe(expectedMessage.StreamVersion);
                message.Type.ShouldBe(expectedMessage.Type);

                // We don't care about StreamMessage.Position and StreamMessage.Position
                // as they are non-deterministic
            }
        }

        /*[Fact, Trait("Category", "ReadAll")]
        public async Task Can_read_all_backwards_without_prefetch()
        {
            using (var fixture = GetFixture())
            {
                using (var store = await fixture.GetStreamStore())
                {
                    await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                    var page = await store.ReadAllBackwards(Position.End, 4, prefetchJsonData: false);

                    foreach (var streamMessage in page.Messages)
                    {
                        streamMessage.GetJsonData().IsCompleted.ShouldBeFalse();

                        (await streamMessage.GetJsonData()).ShouldNotBeNullOrWhiteSpace();
                    }
                }
            }
        }*/

        [Theory, Trait("Category", "ReadAll")]
        [InlineData(3, 0, 3, 3, 0, 3)] // Read entire store
        [InlineData(3, 0, 4, 3, 0, 3)] // Read entire store
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
            int expectedNextPosition)
        {
            await store.AppendToStream(
                "stream-1",
                ExpectedVersion.NoStream,
                CreateNewStreamMessageSequence(1, numberOfSeedMessages));

            var page = store.ReadAllForwards(fromPosition, maxCount);

            (await page.Take(expectedCount).CountAsync()).ShouldBe(expectedCount);
            page.FromPosition.ShouldBe(expectedFromPosition);
            page.NextPosition.ShouldBe(expectedNextPosition);
        }

        [Theory, Trait("Category", "ReadAll")]
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
            int expectedNextPosition)
        {
            if(numberOfSeedMessages > 0)
            {
                await store.AppendToStream(
                    "stream-1",
                    ExpectedVersion.NoStream,
                    CreateNewStreamMessageSequence(1, numberOfSeedMessages));
            }

            var allMessagesPage = store.ReadAllBackwards(fromPosition, maxCount);

            (await allMessagesPage.Take(expectedCount).CountAsync()).ShouldBe(expectedCount);
            allMessagesPage.FromPosition.ShouldBe(expectedFromPosition);
            allMessagesPage.NextPosition.ShouldBe(expectedNextPosition);
        }

        [Theory, Trait("Category", "ReadAll")]
        [InlineData("stream/1")]
        [InlineData("stream%1")]
        public async Task when_read_all_forwards_with_url_encodable_stream(string expectedStreamId)
        {
            await store.AppendToStream(expectedStreamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1));

            var result = store.ReadAllForwards(Position.Start, 1);

            var streamId = await result.Select(message => message.StreamId).FirstOrDefaultAsync();

            Assert.Equal(expectedStreamId, streamId);
        }

        [Theory, Trait("Category", "ReadAll")]
        [InlineData("stream/1")]
        [InlineData("stream%1")]
        public async Task when_read_all_backwards_with_url_encodable_stream(string expectedStreamId)
        {
            await store.AppendToStream(expectedStreamId, ExpectedVersion.NoStream, CreateNewStreamMessages(1));

            var result = store.ReadAllBackwards(Position.End, 1);

            var streamId = await result.Select(message => message.StreamId).FirstOrDefaultAsync();

            Assert.Equal(expectedStreamId, streamId);
        }
    }
}