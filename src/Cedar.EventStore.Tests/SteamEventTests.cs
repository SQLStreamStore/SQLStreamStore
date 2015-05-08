namespace Cedar.EventStore
{
    using System;
    using FluentAssertions;
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
                "cp",
                DateTime.UtcNow,
                "type",
                "\"data\"",
                "\"meta\"");

            streamEvent.JsonDataAs<string>().Should().Be("data");
            streamEvent.JsonMetadataAs<string>().Should().Be("meta");
        }
    }
}