namespace SqlStreamStore.InMemory
{
    using System;
    using System.Linq;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class InMemoryTests
    {
        [Fact]
        public async void When_reading_a_stream_backwards_with_gaps_and_providing_the_fromVersion_explicitly()
        {
            var streamStore = new InMemoryStreamStore();
            var streamId = new StreamId("streamid");
            var messageOne = new NewStreamMessage(Guid.NewGuid(), "type", "jsondata1");
            var messageTwo = new NewStreamMessage(Guid.NewGuid(), "type", "jsondata2");
            var messageThree = new NewStreamMessage(Guid.NewGuid(), "type", "jsondata3");

            var ap1 = await streamStore.AppendToStream(streamId, ExpectedVersion.Any, messageOne);
            var ap2 = await streamStore.AppendToStream(streamId, ExpectedVersion.Any, messageTwo);
            var ap3 = await streamStore.AppendToStream(streamId, ExpectedVersion.Any, messageThree);

            await streamStore.DeleteMessage(streamId, messageTwo.MessageId);

            var page = await streamStore.ReadStreamBackwards(streamId, ap3.CurrentVersion, 10);
            page.Messages.Length.ShouldBe(2);
            var jsonDataOfMessageOne = await page.Messages.First().GetJsonData();
            var jsonDataOfMessageThree = await page.Messages.Last().GetJsonData();
            jsonDataOfMessageOne.ShouldBe("jsondata3");
            jsonDataOfMessageThree.ShouldBe("jsondata1");
        }
    }
}