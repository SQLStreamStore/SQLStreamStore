namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.HalClient.Http;

    public partial class HttpClientSqlStreamStore : IStreamStore
    {
        private readonly HttpClient _httpClient;

        public HttpClientSqlStreamStore(HttpClientSqlStreamStoreSettings settings)
        {
            if(settings.BaseAddress == null)
            {
                throw new ArgumentNullException(nameof(settings.BaseAddress));
            }
            if(!settings.BaseAddress.ToString().EndsWith("/"))
            {
                throw new ArgumentException("BaseAddress must end with /", nameof(settings.BaseAddress));
            }
            
            _httpClient = new HttpClient(settings.HttpMessageHandler)
            {
                BaseAddress = settings.BaseAddress,
                DefaultRequestHeaders = { Accept = { new MediaTypeWithQualityHeaderValue("application/hal+json") } }
            };
        }

        public void Dispose()
        {
            OnDispose?.Invoke();
            _httpClient.Dispose();
        }

        public async Task<long> ReadHeadPosition(CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = new JsonHttpClient(_httpClient);
            var response = await client.HeadAsync("/stream", cancellationToken);

            response.EnsureSuccessStatusCode();

            response.Headers.TryGetValues("SSS-HeadPosition", out var headPositionHeaders);
            
            if(!long.TryParse(headPositionHeaders.Single(), out var headPosition))
            {
                throw new InvalidOperationException();
            }

            return headPosition;
        }

        public event Action OnDispose;
    }
}