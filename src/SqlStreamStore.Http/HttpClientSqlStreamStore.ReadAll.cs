namespace SqlStreamStore
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.HalClient;
    using SqlStreamStore.HalClient.Models;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public async Task<ReadAllPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = CreateClient();

            client = await client.RootAsync(
                LinkFormatter.ReadAllForwards(fromPositionInclusive, maxCount, prefetchJsonData),
                cancellationToken);

            return ReadAllForwardsInternal(client, prefetchJsonData);
        }

        public async Task<ReadAllPage> ReadAllBackwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = CreateClient();

            client = await client.RootAsync(
                LinkFormatter.ReadAllBackwards(fromPositionInclusive, maxCount, prefetchJsonData),
                cancellationToken);

            return ReadAllBackwardsInternal(client, prefetchJsonData);
        }

        private static ReadAllPage ReadAllForwardsInternal(IHalClient client, bool prefetch)
        {
            var resource = client.Current.First();

            var pageInfo = resource.Data<HalReadAllPage>();

            var streamMessages = Convert(
                resource.Embedded
                    .Where(r => r.Rel == Constants.Relations.Message)
                    .Reverse()
                    .ToArray(),
                client,
                prefetch);

            var readAllPage = new ReadAllPage(
                pageInfo.FromPosition,
                pageInfo.NextPosition,
                pageInfo.IsEnd,
                ReadDirection.Forward,
                async (position, token) => ReadAllForwardsInternal(
                    await client.GetAsync(resource, Constants.Relations.Next),
                    prefetch),
                streamMessages);

            return readAllPage;
        }

        private static ReadAllPage ReadAllBackwardsInternal(IHalClient client, bool prefetch)
        {
            var resource = client.Current.First();

            var pageInfo = resource.Data<HalReadAllPage>();

            var streamMessages = Convert(
                resource.Embedded
                    .Where(r => r.Rel == Constants.Relations.Message)
                    .ToArray(),
                client,
                prefetch);

            var readAllPage = new ReadAllPage(
                pageInfo.FromPosition,
                pageInfo.NextPosition,
                pageInfo.IsEnd,
                ReadDirection.Backward,
                async (position, cancellationToken) => ReadAllBackwardsInternal(
                    await client.GetAsync(resource, Constants.Relations.Previous, cancellationToken),
                    prefetch),
                streamMessages);

            return readAllPage;
        }
    }
}