namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using Shouldly;
    using Xunit;

    public class LongExtensionTests
    {
        [Fact]
        public void Correctly_creates_range_inclusive_of_inputs()
        {
            4L.RangeTo(8).ShouldBe(new long[] { 4, 5, 6, 7, 8 });
        }

        [Fact]
        public void Correctly_creates_range_of_single_item()
        {
            4L.RangeTo(4L).ShouldBe(new long[] { 4 });
        }

        [Fact]
        public void Throws_on_reverse_range()
        {
            Assert.Throws<ArgumentException>(() => 4L.RangeTo(3).ToList());
        }
    }
}