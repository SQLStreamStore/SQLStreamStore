namespace SqlStreamStore
{
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Internal.HoneyBearHalClient;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public async Task<ReadStreamPage> ReadStreamForwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            var client = CreateClient();

            client = await client.RootAsync(
                LinkFormatter.ReadStreamForwards(streamId, fromVersionInclusive, maxCount, prefetchJsonData),
                cancellationToken);

            return ReadStreamForwardsInternal(
                client,
                streamId,
                fromVersionInclusive,
                prefetchJsonData);
        }

        public async Task<ReadStreamPage> ReadStreamBackwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            var client = CreateClient();

            client = await client.RootAsync(
                LinkFormatter.ReadStreamBackwards(streamId, fromVersionInclusive, maxCount, prefetchJsonData),
                cancellationToken);

            return ReadStreamBackwardsInternal(
                client,
                streamId,
                fromVersionInclusive,
                prefetchJsonData);
        }

        private static ReadStreamPage ReadStreamForwardsInternal(
            IHalClient client,
            StreamId streamId,
            int fromVersionInclusive,
            bool prefetchJsonData)
        {
            var resource = client.Current.First();

            if(client.StatusCode == HttpStatusCode.NotFound)
            {
                return new ReadStreamPage(
                    streamId,
                    PageReadStatus.StreamNotFound,
                    fromVersionInclusive,
                    -1,
                    -1,
                    -1,
                    ReadDirection.Forward,
                    true);
            }

            var pageInfo = resource.Data<HalReadPage>();

            var streamMessages = Convert(
                resource.Embedded
                    .Where(r => r.Rel == Constants.Relations.Message)
                    .Reverse()
                    .ToArray(),
                client,
                prefetchJsonData);

            var readStreamPage = new ReadStreamPage(
                streamId,
                PageReadStatus.Success,
                pageInfo.FromStreamVersion,
                pageInfo.NextStreamVersion,
                pageInfo.LastStreamVersion,
                pageInfo.LastStreamPosition,
                ReadDirection.Forward,
                pageInfo.IsEnd,
                ReadNextStreamPage,
                streamMessages);

            return readStreamPage;

            async Task<ReadStreamPage> ReadNextStreamPage(int nextVersion, CancellationToken ct)
                => resource.Links.Any(link => link.Rel == Constants.Relations.Next)
                    ? ReadStreamForwardsInternal(
                        await client.GetAsync(resource, Constants.Relations.Next),
                        streamId,
                        pageInfo.LastStreamVersion,
                        prefetchJsonData)
                    : new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        pageInfo.LastStreamVersion,
                        nextVersion,
                        pageInfo.LastStreamVersion,
                        pageInfo.LastStreamPosition,
                        ReadDirection.Forward,
                        true,
                        ReadNextStreamPage);
        }

        private static ReadStreamPage ReadStreamBackwardsInternal(
            IHalClient client,
            StreamId streamId,
            int fromVersionInclusive,
            bool prefetchJsonData)
        {
            var resource = client.Current.First();

            if(client.StatusCode == HttpStatusCode.NotFound)
            {
                return new ReadStreamPage(
                    streamId,
                    PageReadStatus.StreamNotFound,
                    fromVersionInclusive,
                    -1,
                    -1,
                    -1,
                    ReadDirection.Backward,
                    true);
            }

            var pageInfo = resource.Data<HalReadPage>();

            var streamMessages = Convert(
                resource.Embedded
                    .Where(r => r.Rel == Constants.Relations.Message)
                    .ToArray(),
                client,
                prefetchJsonData);

            var readStreamPage = new ReadStreamPage(
                streamId,
                PageReadStatus.Success,
                pageInfo.FromStreamVersion,
                pageInfo.NextStreamVersion,
                pageInfo.LastStreamVersion,
                pageInfo.LastStreamPosition,
                ReadDirection.Backward,
                pageInfo.IsEnd,
                async (nextVersion, token) => ReadStreamBackwardsInternal(
                    await client.GetAsync(resource, Constants.Relations.Previous),
                    streamId,
                    pageInfo.LastStreamVersion,
                    prefetchJsonData),
                streamMessages);

            return readStreamPage;
        }
    }
}