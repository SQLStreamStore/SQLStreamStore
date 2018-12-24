namespace SqlStreamStore
{
    using Xunit.Abstractions;

    public class HttpClientStreamStoreAcceptanceTests : StreamStoreAcceptanceTests
    {
        public HttpClientStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override StreamStoreAcceptanceTestFixture GetFixture()
            => new HttpClientStreamStoreFixture();
    }
}