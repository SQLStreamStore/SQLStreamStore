namespace SqlStreamStore.V1.Infrastructure
{
    using System;
    using Shouldly;
    using SqlStreamStore.V1.Streams;
    using Xunit;

    public class StreamIdTests
    {
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("s s")]
        public void When_invalid_then_should_throw(string value)
        {
            Action act = () => new StreamId(value);

            act.ShouldThrow<ArgumentException>();
        }

        [Fact]
        public void Is_equatable()
        {
            (new StreamId("foo") == new StreamId("foo")).ShouldBeTrue();
            new StreamId("foo").Equals(new StreamId("foo")).ShouldBeTrue();
            (new StreamId("foo") != new StreamId("bar")).ShouldBeTrue();
            new StreamId("foo").GetHashCode().ShouldBe(new StreamId("foo").GetHashCode());
        } 
    }
}