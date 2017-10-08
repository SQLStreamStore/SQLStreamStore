namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.HalClient;
    using SqlStreamStore.HalClient.Models;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public async Task<AppendResult> AppendToStream(
            StreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
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

            client = await client.Post(
                "streamStore:appendToStream",
                messages,
                null,
                null,
                new Dictionary<string, string[]>
                {
                    [Constants.Headers.ExpectedVersion] = new[] { $"{expectedVersion}" }
                },
                cancellationToken);

            ThrowOnError(client);

            return default(AppendResult);
        }
    }
}