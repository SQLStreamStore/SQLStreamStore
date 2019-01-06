namespace SqlStreamStore
{
    using System;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.TestHost;
    using SqlStreamStore.HAL;
    using SqlStreamStore.Infrastructure;

    public class HttpClientStreamStoreFixture2 : IStreamStoreFixture
    {
        private readonly InMemoryStreamStore _inMemoryStreamStore;
        private readonly TestServer _server;

        public HttpClientStreamStoreFixture2()
        {
            _inMemoryStreamStore = new InMemoryStreamStore(() => GetUtcNow());

            var webHostBuilder = new WebHostBuilder()
                .Configure(builder => builder.UseSqlStreamStoreHal(_inMemoryStreamStore));

            _server = new TestServer(webHostBuilder);

            var handler = new RedirectingHandler
            {
                InnerHandler = _server.CreateHandler()
            };

            Store = new HttpClientSqlStreamStore(
                new HttpClientSqlStreamStoreSettings
                {
                    GetUtcNow = () => GetUtcNow(),
                    HttpMessageHandler = handler,
                    BaseAddress = new UriBuilder().Uri
                });
        }

        public void Dispose()
        {
            Store.Dispose();
            _server.Dispose();
            _inMemoryStreamStore.Dispose();
        }

        public IStreamStore Store { get; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;
    }
}