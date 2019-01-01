namespace SqlStreamStore.HAL.StreamBrowser
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Halcyon.HAL;
    using SqlStreamStore.HAL.StreamBrowser;

    internal class StreamBrowserResource
    {
        private readonly IStreamStore _streamStore;

        public StreamBrowserResource(IStreamStore streamStore)
        {
            _streamStore = streamStore;
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
                                    .Add(Constants.Relations.Feed, $"{Constants.Streams.Stream}/{streamId}", streamId)
                                    .Self()))));
        }
    }
}