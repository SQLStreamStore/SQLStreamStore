 // ReSharper disable once CheckNamespace
namespace SqlStreamStore
{
    using SqlStreamStore.InMemory;
    using Xunit.Abstractions;

    public class InMemoryStreamStoreAcceptanceTests : StreamStoreAcceptanceTests
    {
        public InMemoryStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override  StreamStoreAcceptanceTestFixture GetFixture()
            => new InMemoryStreamStoreFixture();
    }
}