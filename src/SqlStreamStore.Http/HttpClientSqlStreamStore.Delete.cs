namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Internal.HoneyBearHalClient;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public async Task DeleteStream(
            StreamId streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default)
        {
            var client = CreateClient(new Resource
            {
                Links =
                {
                    new Link
                    {
                        Href = LinkFormatter.Stream(streamId),
                        Rel = Constants.Relations.AppendToStream
                    }
                }
            });

            client = await client.Delete(
                Constants.Relations.AppendToStream,
                null,
                null,
                new Dictionary<string, string[]>
                {
                    [Constants.Headers.ExpectedVersion] = new[] { $"{expectedVersion}" }
                },
                cancellationToken);

            ThrowOnError(client);
        }

        public async Task DeleteMessage(
            StreamId streamId,
            Guid messageId,
            CancellationToken cancellationToken = default)
        {
            var client = CreateClient(new Resource
            {
                Links =
                {
                    new Link
                    {
                        Href = LinkFormatter.StreamByMessageId(streamId, messageId),
                        Rel = Constants.Relations.AppendToStream
                    }
                }
            });

            client = await client.Delete(
                Constants.Relations.AppendToStream,
                null,
                null,
                cancellationToken: cancellationToken);

            ThrowOnError(client);
            
        }
    }
}