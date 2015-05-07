namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
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
                "\"data\"",
                new ReadOnlyCollection<byte>(new List<byte>()));

            streamEvent.JsonAs<string>().Should().Be("data");
        }
    }
}