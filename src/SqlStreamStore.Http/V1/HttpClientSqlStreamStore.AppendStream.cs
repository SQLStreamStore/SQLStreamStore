namespace SqlStreamStore.V1
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.V1.Imports.Ensure.That;
    using SqlStreamStore.V1.Internal.HoneyBearHalClient;
    using SqlStreamStore.V1.Internal.HoneyBearHalClient.Models;
    using SqlStreamStore.V1.Streams;

    partial class HttpClientSqlStreamStore
    {
        public async Task<AppendResult> AppendToStream(
            StreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(expectedVersion, nameof(expectedVersion)).IsGte(ExpectedVersion.NoStream);
            Ensure.That(messages, nameof(messages)).IsNotNull();

            GuardAgainstDisposed();

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

            client = await client.Post(
                Constants.Relations.AppendToStream,
                messages,
                null,
                null,
                new Dictionary<string, string[]>
                {
                    [Constants.Headers.ExpectedVersion] = new[] { $"{expectedVersion}" }
                },
                cancellationToken);

            ThrowOnError(client);

            var resource = client.Current.First();

            return resource.Data<HalAppendResult>();
        }
    }
}