 // ReSharper disable once CheckNamespace
namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using SqlStreamStore.InMemory;
    using Xunit.Abstractions;

    public class InMemoryStreamStoreAcceptanceTests : AcceptanceTests
    {
        public InMemoryStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override  StreamStoreAcceptanceTestFixture GetFixture()
            => new InMemoryStreamStoreFixture();

        protected override Task<IStreamStoreFixture> CreateFixture() 
            => Task.FromResult<IStreamStoreFixture>(new InMemoryStreamStoreFixture2());
    }
}