namespace SqlStreamStore.V1.Internal.HoneyBearHalClient.Http
{
    using System;
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
        private readonly Func<HttpClient> _clientFactory;
        private static readonly RecyclableMemoryStreamManager s_streamManager = new RecyclableMemoryStreamManager();
        private readonly JsonSerializer _serializer;
        public Uri BaseAddress { get; }

        public JsonHttpClient(Func<HttpClient> clientFactory, JsonSerializer serializer, Uri baseAddress)
        {
            _clientFactory = clientFactory;
            _serializer = serializer;
            BaseAddress = baseAddress;
        }

        public async Task<HttpResponseMessage> GetAsync(
            string uri,
            CancellationToken cancellationToken = default)
        {
            using(var client = _clientFactory())
            {
                return await client.GetAsync(uri, cancellationToken);
            }
        }

        public async Task<HttpResponseMessage> HeadAsync(
            string uri,
            CancellationToken cancellationToken = default)
        {
            using(var client = _clientFactory())
            using(var request = new HttpRequestMessage(HttpMethod.Head, uri))
            {
                return await client.SendAsync(request, cancellationToken);
            }
        }

        public async Task<HttpResponseMessage> OptionsAsync(
            string uri,
            CancellationToken cancellationToken = default)
        {
            using(var client = _clientFactory())
            {
                return await client.SendAsync(new HttpRequestMessage(HttpMethod.Options, uri), cancellationToken);
            }
        }

        public async Task<HttpResponseMessage> PostAsync<T>(
            string uri,
            T value,
            IDictionary<string, string[]> headers,
            CancellationToken cancellationToken = default)
        {
            using(var stream = s_streamManager.GetStream())
            using(var writer = new JsonTextWriter(new StreamWriter(stream))
            {
                CloseOutput = false
            })
            {
                var token = JToken.FromObject(value, _serializer);

                await token.WriteToAsync(writer, cancellationToken);

                await writer.FlushAsync(cancellationToken);

                stream.Position = 0;

                using(var client = _clientFactory())
                using(var request = new HttpRequestMessage(HttpMethod.Post, uri)
                {
                    Content = new StreamContent(stream)
                        { Headers = { ContentType = new MediaTypeHeaderValue("application/json") } }
                })
                {
                    CopyHeaders(headers, request);

                    return await client.SendAsync(request, cancellationToken);
                }
            }
        }

        public async Task<HttpResponseMessage> DeleteAsync(
            string uri,
            IDictionary<string, string[]> headers,
            CancellationToken cancellationToken = default)
        {
            using(var client = _clientFactory())
            using(var request = new HttpRequestMessage(HttpMethod.Delete, uri))
            {
                CopyHeaders(headers, request);

                return await client.SendAsync(request, cancellationToken);
            }
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