namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.HalClient;
    using SqlStreamStore.HalClient.Models;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public async Task DeleteStream(
            StreamId streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = CreateClient(new Resource
            {
                Links =
                {
                    new Link
                    {
                        Href = LinkFormatter.Stream(streamId),
                        Rel = "streamStore:appendToStream"
                    }
                }
            });

            client = await client.Delete(
                "streamStore:appendToStream",
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
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = CreateClient(new Resource
            {
                Links =
                {
                    new Link
                    {
                        Href = LinkFormatter.StreamByMessageId(streamId, messageId),
                        Rel = "streamStore:appendToStream"
                    }
                }
            });

            client = await client.Delete(
                "streamStore:appendToStream",
                null,
                null,
                cancellationToken: cancellationToken);

            ThrowOnError(client);
            
        }
    }
}