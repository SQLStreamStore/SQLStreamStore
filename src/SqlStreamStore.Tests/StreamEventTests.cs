namespace SqlStreamStore
{
    using System;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class messageTests
    {
        [Fact]
        public void Can_deserialize()
        {
            var message = new StreamMessage(
                "stream",
                Guid.NewGuid(),
                1,
                2,
                DateTime.UtcNow,
                "type",
                "\"data\"",
                "\"meta\"");

            message.JsonDataAs<string>().ShouldBe("data");
            message.JsonMetadataAs<string>().ShouldBe("meta");
        }
    }
}