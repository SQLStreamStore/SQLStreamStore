namespace SqlStreamStore
{
    using System;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.TestHost;
    using SqlStreamStore.HAL;
    using MidFunc = System.Func<
        System.Func<
            System.Collections.Generic.IDictionary<string, object>,
            System.Threading.Tasks.Task>,
        System.Func<
            System.Collections.Generic.IDictionary<string, object>,
            System.Threading.Tasks.Task>>;

    public class HttpClientStreamStoreFixture : StreamStoreAcceptanceTestFixture, IDisposable
    {
        private readonly InMemoryStreamStore _innerStreamStore;
        private readonly HttpMessageHandler _messageHandler;
        private readonly TestServer _server;

        public HttpClientStreamStoreFixture()
        {
            _innerStreamStore = new InMemoryStreamStore(() => GetUtcNow());

            _server = new TestServer(
                new WebHostBuilder().Configure(builder => builder.UseSqlStreamStoreHal(_innerStreamStore)));

            _messageHandler = _server.CreateHandler();
        }

        public override long MinPosition => 0;

        public override int MaxSubscriptionCount => 500;

        public override Task<IStreamStore> GetStreamStore()
            => Task.FromResult<IStreamStore>(
                new HttpClientSqlStreamStore(
                    new HttpClientSqlStreamStoreSettings
                    {
                        GetUtcNow = () => GetUtcNow(),
                        HttpMessageHandler = _messageHandler,
                        BaseAddress = new UriBuilder().Uri
                    }));

        void IDisposable.Dispose()
        {
            _server?.Dispose();
            _messageHandler?.Dispose();
            _innerStreamStore?.Dispose();
        }
    }
}