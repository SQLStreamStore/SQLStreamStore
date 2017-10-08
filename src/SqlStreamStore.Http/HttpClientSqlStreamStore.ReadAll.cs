namespace SqlStreamStore
{
    using System;
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
                LinkFormatter.ReadAllForwards(fromPositionInclusive, maxCount, prefetchJsonData));

            return ReadAllForwardsInternal(client, cancellationToken);
        }

        public async Task<ReadAllPage> ReadAllBackwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = CreateClient();

            client = await client.RootAsync(
                LinkFormatter.ReadAllBackwards(fromPositionInclusive, maxCount, prefetchJsonData));

            return ReadAllBackwardsInternal(client, cancellationToken);
        }

        private static ReadAllPage ReadAllForwardsInternal(IHalClient client, CancellationToken cancellationToken)
        {
            var resource = client.Current.First();

            var streamMessages = Convert(
                resource.Embedded.Where(r => r.Rel == "streamStore:message").ToArray());

            var readAllPage = new ReadAllPage(
                streamMessages.First().Position,
                streamMessages.Last().Position + 1,
                resource.Links.Any(link => link.Rel == "next"),
                ReadDirection.Forward,
                async (position, token) => ReadAllForwardsInternal(
                    await client.GetAsync(resource, "next"),
                    cancellationToken),
                streamMessages);

            return readAllPage;
        }
        
        private static ReadAllPage ReadAllBackwardsInternal(IHalClient client, CancellationToken cancellationToken)
        {
            var resource = client.Current.First();

            var streamMessages = Convert(
                resource.Embedded.Where(r => r.Rel == "streamStore:message").ToArray());

            var last = resource.Links.FirstOrDefault(link => link.Rel == "last");
            var self = resource.Links.FirstOrDefault(link => link.Rel == "self");
            
            var readAllPage = new ReadAllPage(
                streamMessages.First().Position,
                streamMessages.Last().Position - 1,
                last?.Href == self?.Href,
                ReadDirection.Forward,
                async (position, token) => ReadAllForwardsInternal(
                    await client.GetAsync(resource, "next"),
                    cancellationToken),
                streamMessages);

            return readAllPage;
        }


        private static StreamMessage[] Convert(IResource[] streamMessages)
            => Array.ConvertAll(streamMessages, message => (StreamMessage)message.Data<HttpStreamMessage>());
    }
}