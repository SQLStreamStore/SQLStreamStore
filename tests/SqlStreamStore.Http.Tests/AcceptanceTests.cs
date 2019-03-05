namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Xunit.Abstractions;

    public class AcceptanceTests : AcceptanceTestsBase
    {
        public AcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override Task<IStreamStoreFixture> CreateFixture()
            => Task.FromResult<IStreamStoreFixture>(new HttpClientStreamStoreFixture());
    }
}