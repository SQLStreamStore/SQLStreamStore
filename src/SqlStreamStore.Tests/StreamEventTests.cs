namespace SqlStreamStore
{
    using System;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class StreamEventTests
    {
        [Fact]
        public void Can_deserialize()
        {
            var streamEvent = new StreamEvent(
                "stream",
                Guid.NewGuid(),
                1,
                2,
                DateTime.UtcNow,
                "type",
                "\"data\"",
                "\"meta\"");

            streamEvent.JsonDataAs<string>().ShouldBe("data");
            streamEvent.JsonMetadataAs<string>().ShouldBe("meta");
        }
    }
}