namespace SqlStreamStore.HAL.StreamBrowser
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Halcyon.HAL;
    using SqlStreamStore.HAL.StreamBrowser;
    using SqlStreamStore.Streams;

    internal class StreamBrowserResource : IResource
    {
        private readonly IStreamStore<ReadAllPage> _streamStore;

        public SchemaSet Schema { get; }

        public StreamBrowserResource(IStreamStore<ReadAllPage> streamStore)
        {
            _streamStore = streamStore;
            Schema = new SchemaSet<StreamBrowserResource>();
        }

        public async Task<Response> Get(ListStreamsOperation operation, CancellationToken cancellationToken)
        {
            var listStreamsPage = await operation.Invoke(_streamStore, cancellationToken);

            return new HalJsonResponse(new HALResponse(new
                {
                    listStreamsPage.ContinuationToken
                })
                .AddLinks(
                    Links
                        .FromOperation(operation)
                        .Index()
                        .Find()
                        .StreamBrowserNavigation(listStreamsPage, operation))
                .AddEmbeddedCollection(
                    Constants.Relations.Feed,
                    Array.ConvertAll(
                        listStreamsPage.StreamIds,
                        streamId => new HALResponse(null)
                            .AddLinks(
                                Links
                                    .FromOperation(operation)
                                    .Add(Constants.Relations.Feed, LinkFormatter.Stream(streamId), streamId)
                                    .Self()))));
        }
    }
}