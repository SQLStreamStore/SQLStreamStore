namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;
    using SqlStreamStore.HalClient;
    using SqlStreamStore.HalClient.Models;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public async Task<StreamMetadataResult> GetStreamMetadata(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = CreateClient(new Resource
            {
                Links =
                {
                    new Link
                    {
                        Href = LinkFormatter.Stream(streamId)
                    }
                }
            });

            client = await client.GetAsync(client.Current.First(), null);

            if(client.StatusCode != HttpStatusCode.NotFound)
            {
                ThrowOnError(client);
            }

            client = await client.GetAsync(client.Current.First(), "streamStore:metadata");

            var resource = client.Current.First();

            return resource.Data<HalStreamMetadataResult>();
        }

        public async Task SetStreamMetadata(
            StreamId streamId,
            int expectedStreamMetadataVersion = ExpectedVersion.Any,
            int? maxAge = null,
            int? maxCount = null,
            string metadataJson = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = CreateClient(new Resource
            {
                Links =
                {
                    new Link
                    {
                        Href = LinkFormatter.Stream(streamId)
                    }
                }
            });

            client = await client.GetAsync(client.Current.First(), null);

            if(client.StatusCode != HttpStatusCode.NotFound)
            {
                ThrowOnError(client);
            }

            client = await client.GetAsync(client.Current.First(), "streamStore:metadata");

            if(client.StatusCode != HttpStatusCode.NotFound)
            {
                ThrowOnError(client);
            }

            var metadata = new Dictionary<string, object>
            {
                ["maxAge"] = maxAge,
                ["maxCount"] = maxCount
            };

            if(!string.IsNullOrEmpty(metadataJson))
            {
                metadata["metadataJson"] = TryParseMetadataJson(metadataJson);
            }

            client = await client.Post("self",
                metadata,
                null,
                null,
                new Dictionary<string, string[]>
                {
                    [Constants.Headers.ExpectedVersion] = new[] { $"{expectedStreamMetadataVersion}" }
                },
                cancellationToken);

            ThrowOnError(client);
        }

        private static object TryParseMetadataJson(string metadataJson)
        {
            try
            {
                return JToken.Parse(metadataJson);
            }
            catch(Newtonsoft.Json.JsonException)
            {
                return metadataJson;
            }
        }
    }
}