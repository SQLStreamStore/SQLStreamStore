﻿namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;
    using SqlStreamStore.Internal.HoneyBearHalClient;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public sealed partial class HttpClientSqlStreamStore : IStreamStore, IReadonlyStreamStore<ReadAllPage>
    {
        private static readonly JsonSerializer s_serializer = JsonSerializer.Create(new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.None,
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            Converters =
            {
                new NewStreamMessageConverter()
            }
        });

        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;
        private readonly HttpClientSqlStreamStoreSettings _settings;
        private bool _disposed;

        public HttpClientSqlStreamStore(HttpClientSqlStreamStoreSettings settings)
        {
            _settings = settings;
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
            {
                if(settings.CreateStreamStoreNotifier == null)
                {
                    throw new InvalidOperationException(
                        "Cannot create notifier because supplied createStreamStoreNotifier was null");
                }

                return settings.CreateStreamStoreNotifier.Invoke(this);
            });
        }

        public void Dispose()
        {
            _disposed = true;
            OnDispose?.Invoke();
        }

        public async Task<long> ReadHeadPosition(CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            var client = CreateClient();
            var response = await client.Client.HeadAsync(LinkFormatter.AllStream(), cancellationToken);

            response.EnsureSuccessStatusCode();

            response.Headers.TryGetValues(Constants.Headers.HeadPosition, out var headPositionHeaders);

            if(!long.TryParse(headPositionHeaders.Single(), out var headPosition))
            {
                throw new InvalidOperationException();
            }

            return headPosition;
        }

        public async Task<long> ReadStreamHeadPosition(StreamId streamId, CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            return (await ReadStreamBackwards(streamId, StreamVersion.End, 1, false, cancellationToken)).LastStreamPosition;
        }

        public async Task<int> ReadStreamHeadVersion(StreamId streamId, CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            return (await ReadStreamBackwards(streamId, StreamVersion.End, 1, false, cancellationToken)).LastStreamVersion;
        }

        private static void ThrowOnError(IHalClient client)
        {
            switch(client.StatusCode ?? default)
            {
                case default(HttpStatusCode):
                    return;
                case HttpStatusCode.Conflict:
                    var resource = client.Current.First();
                    throw new WrongExpectedVersionException(resource.Data<HttpError>().Detail);
                case var status when status >= HttpStatusCode.BadRequest:
                    throw new HttpRequestException($"Response status code does not indicate success: {status}");
                default:
                    return;
            }
        }

        public event Action OnDispose;

        private IHalClient CreateClient(IResource resource) =>
            new HalClient(
                CreateClient(),
                new[] { resource.WithBaseAddress(_settings.BaseAddress) });

        private IHalClient CreateClient() => new HalClient(CreateHttpClient, s_serializer, _settings.BaseAddress);

        private static StreamMessage[] Convert(IResource[] streamMessages, IHalClient client, bool prefetch = false)
            => Array.ConvertAll(
                streamMessages,
                streamMessage =>
                {
                    var httpStreamMessage = streamMessage.Data<HttpStreamMessage>();

                    return httpStreamMessage.ToStreamMessage(
                        ct =>
                            prefetch
                                ? Task.FromResult(httpStreamMessage.Payload.ToString())
                                : Task.Run(() => GetPayload(client, streamMessage, ct), ct));
                });

        private static async Task<string> GetPayload(
            IHalClient client,
            IResource streamMessage,
            CancellationToken cancellationToken)
            => (await client.GetAsync(streamMessage, Constants.Relations.Self, cancellationToken))
                .Current.FirstOrDefault()?.Data<HttpStreamMessage>()?.Payload?.ToString();

        private HttpClient CreateHttpClient()
        {
            var baseAddress = _settings.BaseAddress;

            if(baseAddress == null)
            {
                throw new ArgumentNullException(nameof(baseAddress));
            }

            if(!baseAddress.ToString().EndsWith("/"))
            {
                throw new ArgumentException("BaseAddress must end with /", nameof(baseAddress));
            }

            var client = _settings.CreateHttpClient();

            client.BaseAddress = baseAddress;
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/hal+json"));

            return client;
        }

        private void GuardAgainstDisposed()
        {
            if(_disposed)
            {
                throw new ObjectDisposedException(nameof(HttpClientSqlStreamStore));
            }
        }
    }
}