namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using FluentAssertions;
    using Xunit;

    public class MetadataTests
    {
        [Fact]
        public void Can_set_and_get_metadata()
        {
            var metadata = new Metadata();

            metadata["key"] = "value";

            metadata.Get<string>("key").Should().Be("value");
        }

        [Fact]
        public void When_key_does_not_exist_and_get_then_should_throw()
        {
            var metadata = new Metadata();

            Action act = () => metadata.Get<string>("key");

            act.ShouldThrow<KeyNotFoundException>();
        }
    }
}