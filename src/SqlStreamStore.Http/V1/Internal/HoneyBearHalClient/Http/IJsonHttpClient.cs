namespace SqlStreamStore.V1.Internal.HoneyBearHalClient.Http
{
    using System;
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;

    internal interface IJsonHttpClient
    {
        Uri BaseAddress { get; }
        Task<HttpResponseMessage> GetAsync(string uri, CancellationToken cancellationToken = default);

        Task<HttpResponseMessage> HeadAsync(string uri, CancellationToken cancellationToken = default);

        Task<HttpResponseMessage> OptionsAsync(string uri, CancellationToken cancellationToken = default);

        Task<HttpResponseMessage> PostAsync<T>(string uri, T value, IDictionary<string, string[]> headers, CancellationToken cancellationToken = default);

        Task<HttpResponseMessage> DeleteAsync(string uri, IDictionary<string, string[]> headers, CancellationToken cancellationToken = default);
    }
}
