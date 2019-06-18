namespace SqlStreamStore.V1.Infrastructure
{
    using System;
    using Shouldly;
    using SqlStreamStore.V1.Streams;
    using Xunit;

    public class DeterministicGuidGeneratorTests
    {
        [Fact]
        public void Given_same_input_should_generate_same_Guid()
        {
            var generator = new DeterministicGuidGenerator(Guid.NewGuid());
            var guid1 = generator.Create("stream-1", ExpectedVersion.Any, "some-data");
            var guid2 = generator.Create("stream-1", ExpectedVersion.Any, "some-data");

            guid2.ShouldBe(guid1);
        }

        [Fact]
        public void Given_different_input_should_generate_different_Guid()
        {
            var generator = new DeterministicGuidGenerator(Guid.NewGuid());
            var guid1 = generator.Create("stream-1", ExpectedVersion.Any, "some-data");
            var guid2 = generator.Create("stream-1", 1, "some-data");
            guid2.ShouldNotBe(guid1);
        }
    }
}