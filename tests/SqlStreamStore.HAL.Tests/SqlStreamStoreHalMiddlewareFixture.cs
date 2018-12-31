namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Linq;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.TestHost;
    using Microsoft.Extensions.DependencyInjection;
    using SqlStreamStore.Streams;

    internal class SqlStreamStoreHalMiddlewareFixture : IDisposable
    {
        public IStreamStore StreamStore { get; }
        public HttpClient HttpClient { get; }

        private readonly TestServer _server;

        public SqlStreamStoreHalMiddlewareFixture(bool followRedirects = false)
        {
            StreamStore = new InMemoryStreamStore();

            _server = new TestServer(
                new WebHostBuilder()
                    .ConfigureServices(services => services.AddSingleton<IStartup>(new TestStartup(StreamStore)))
                    .UseSetting(WebHostDefaults.ApplicationKey, "WHY"));

            var handler = _server.CreateHandler();
            if(followRedirects)
            {
                handler = new RedirectingHandler
                {
                    InnerHandler = handler
                };
            }

            HttpClient = new HttpClient(handler) { BaseAddress = new UriBuilder().Uri };
            HttpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/hal+json"));
        }

        public void Dispose()
        {
            StreamStore?.Dispose();
            HttpClient?.Dispose();
            _server?.Dispose();
        }

        public Task<AppendResult> WriteNMessages(string streamId, int n)
            => StreamStore.AppendToStream(
                streamId,
                ExpectedVersion.Any,
                Enumerable.Range(0, n)
                    .Select(_ => new NewStreamMessage(Guid.NewGuid(), "type", "{}", "{}"))
                    .ToArray());
    }
}