namespace SqlStreamStore
{
    using System;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading;
    using System.Threading.Tasks;

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

        public Task<long> ReadHeadPosition(CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public event Action OnDispose;
    }
}