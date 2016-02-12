namespace Cedar.EventStore
{
    using System;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public class SteamEventTests
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