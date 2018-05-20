namespace SqlStreamStore.Infrastructure
{
    using System;
    using Shouldly;
    using Xunit;

    public class DeterministicGuidGeneratorTests
    {
        [Fact]
        public void Given_same_input_should_generate_same_Guid()
        {
            var generator = new DeterministicGuidGenerator(Guid.NewGuid());
            var guid1 = generator.Create("some-data");
            var guid2 = generator.Create("some-data");

            guid2.ShouldBe(guid1);
        }

        [Fact]
        public void Given_different_input_should_generate_different_Guid()
        {
            var generator = new DeterministicGuidGenerator(Guid.NewGuid());
            var guid1 = generator.Create("some-data");
            var guid2 = generator.Create("other-data");

            guid2.ShouldNotBe(guid1);
        }
    }
}