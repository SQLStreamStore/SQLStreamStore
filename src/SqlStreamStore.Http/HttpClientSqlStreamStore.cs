namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.HalClient;
    using SqlStreamStore.HalClient.Models;
    using SqlStreamStore.Streams;

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
            var client = CreateClient();
            var response = await client.Client.HeadAsync(LinkFormatter.AllHead, cancellationToken);

            response.EnsureSuccessStatusCode();

            response.Headers.TryGetValues(Constants.Headers.HeadPosition, out var headPositionHeaders);
            
            if(!long.TryParse(headPositionHeaders.Single(), out var headPosition))
            {
                throw new InvalidOperationException();
            }

            return headPosition;
        }

        private static void ThrowOnError(IHalClient client)
        {
            switch(client.StatusCode)
            {
                case var status when (status == default(HttpStatusCode?)):
                    return;
                case HttpStatusCode.Conflict:
                    var resource = client.Current.First();
                    throw new WrongExpectedVersionException(resource.Data<HttpError>().Detail);
                case var status when ((int) status.Value >= 400):
                    throw new HttpRequestException($"Response status code does not indicate success: {status}");
                default:
                    return;
            }
        }

        private IHalClient CreateClient(IResource resource) => new HalClient.HalClient(CreateClient(), new[]{resource});
        private IHalClient CreateClient() => new HalClient.HalClient(_httpClient);

        public event Action OnDispose;
    }
}