namespace SqlStreamStore.HalClient.Http
{
    using System.Collections.Generic;
    using System.IO;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.IO;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal sealed class JsonHttpClient : IJsonHttpClient
    {
        private static readonly RecyclableMemoryStreamManager s_streamManager = new RecyclableMemoryStreamManager();

        public JsonHttpClient(HttpClient client, JsonSerializer serializer)
        {
            HttpClient = client;
            Serializer = serializer;
        }

        public HttpClient HttpClient { get; }
        public JsonSerializer Serializer { get; }

        public Task<HttpResponseMessage> GetAsync(
            string uri,
            CancellationToken cancellationToken = default(CancellationToken))
            => HttpClient.GetAsync(uri, cancellationToken);

        public Task<HttpResponseMessage> HeadAsync(
            string uri,
            CancellationToken cancellationToken = default(CancellationToken))
            => HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Head, uri), cancellationToken);

        public Task<HttpResponseMessage> OptionsAsync(
            string uri,
            CancellationToken cancellationToken = default(CancellationToken))
            => HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Options, uri), cancellationToken);

        public async Task<HttpResponseMessage> PostAsync<T>(
            string uri,
            T value,
            IDictionary<string, string[]> headers,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using(var stream = s_streamManager.GetStream())
            using(var writer = new JsonTextWriter(new StreamWriter(stream))
            {
                CloseOutput = false
            })
            {
                await JToken.FromObject(value, Serializer).WriteToAsync(writer, cancellationToken);

                await writer.FlushAsync(cancellationToken);

                stream.Position = 0;

                var request = new HttpRequestMessage(HttpMethod.Post, uri)
                {
                    Content = new StreamContent(stream)
                    {
                        Headers = { ContentType = new MediaTypeHeaderValue("application/json") }
                    }
                };

                CopyHeaders(headers, request);

                return await HttpClient.SendAsync(request, cancellationToken);
            }
        }

        public Task<HttpResponseMessage> DeleteAsync(
            string uri,
            IDictionary<string, string[]> headers,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var request = new HttpRequestMessage(HttpMethod.Delete, uri);

            CopyHeaders(headers, request);

            return HttpClient.SendAsync(request, cancellationToken);
        }

        private static void CopyHeaders(IDictionary<string, string[]> headers, HttpRequestMessage request)
        {
            if(headers == null)
            {
                return;
            }
            foreach(var header in headers)
            {
                request.Headers.Add(header.Key, header.Value);
            }
        }
    }
}