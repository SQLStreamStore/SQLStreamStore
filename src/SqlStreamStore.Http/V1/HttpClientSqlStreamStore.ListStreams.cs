namespace SqlStreamStore.V1
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.V1.Imports.Ensure.That;
    using SqlStreamStore.V1.Internal.HoneyBearHalClient;
    using SqlStreamStore.V1.Internal.HoneyBearHalClient.Models;
    using SqlStreamStore.V1.Streams;

    partial class HttpClientSqlStreamStore
    {
        public Task<ListStreamsPage> ListStreams(
            int maxCount = 100,
            string continuationToken = default,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(maxCount).IsGt(0);

            return ListStreams(Pattern.Anything(), maxCount, continuationToken, cancellationToken);
        }

        public async Task<ListStreamsPage> ListStreams(
            Pattern pattern,
            int maxCount = 100,
            string continuationToken = default,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(maxCount).IsGt(0);

            GuardAgainstDisposed();

            Task<ListStreamsPage> ListNext(string @continue, CancellationToken ct)
                => ListStreams(pattern, maxCount, @continue, ct);

            var client = CreateClient();

            client = await client.RootAsync(
                continuationToken == default
                    ? LinkFormatter.ListStreams(pattern, maxCount)
                    : LinkFormatter.ListStreams(pattern, maxCount, continuationToken),
                cancellationToken);

            var resource = client.Current.First();

            var streamIds = from e in resource.Embedded
                where e.Rel == Constants.Relations.Feed
                from link in e.Links
                where link.Rel == Constants.Relations.Feed
                select link.Title;

            var listStreams = resource.Data<HalListStreams>();

            return new ListStreamsPage(listStreams.ContinuationToken, streamIds.ToArray(), ListNext);
        }
    }
}