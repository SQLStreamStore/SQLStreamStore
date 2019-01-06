namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Xunit.Abstractions;

    public class HttpClientStreamStoreAcceptanceTests : AcceptanceTests
    {
        public HttpClientStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override StreamStoreAcceptanceTestFixture GetFixture()
            => new HttpClientStreamStoreFixture();

        protected override Task<IStreamStoreFixture> CreateFixture()
            => Task.FromResult<IStreamStoreFixture>(new HttpClientStreamStoreFixture2());
    }
}