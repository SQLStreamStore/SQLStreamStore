namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class messageTests
    {
        [Fact]
        public async Task Can_deserialize()
        {
            var message = new StreamMessage(
                "stream",
                Guid.NewGuid(),
                1,
                2,
                DateTime.UtcNow,
                "type",
                "\"meta\"", "\"data\"");

            (await message.GetJsonDataAs<string>()).ShouldBe("data");
            message.JsonMetadataAs<string>().ShouldBe("meta");
        }
    }
}