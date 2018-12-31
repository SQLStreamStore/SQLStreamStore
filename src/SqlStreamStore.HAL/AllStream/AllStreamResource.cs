namespace SqlStreamStore.HAL.AllStream
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Halcyon.HAL;
    using SqlStreamStore.Streams;

    internal class AllStreamResource : IResource
    {
        private readonly IStreamStore _streamStore;
        private readonly bool _useCanonicalUrls;

        public SchemaSet Schema { get; }

        public AllStreamResource(IStreamStore streamStore, bool useCanonicalUrls)
        {
            if(streamStore == null)
                throw new ArgumentNullException(nameof(streamStore));
            _streamStore = streamStore;
            _useCanonicalUrls = useCanonicalUrls;
        }

        public async Task<Response> Get(
            ReadAllStreamOperation operation,
            CancellationToken cancellationToken)
        {
            if(_useCanonicalUrls && !operation.IsUriCanonical)
            {
                return new HalJsonResponse(new HALResponse(null), 308)
                {
                    Headers = { [Constants.Headers.Location] = new[] { operation.Self } }
                };
            }

            var page = await operation.Invoke(_streamStore, cancellationToken);

            var streamMessages = page.Messages.OrderByDescending(m => m.Position).ToArray();

            var payloads = await Task.WhenAll(
                Array.ConvertAll(
                    streamMessages,
                    message => operation.EmbedPayload
                        ? message.GetJsonData(cancellationToken)
                        : SkippedPayload.Instance));

            var response = new HalJsonResponse(
                new HALResponse(new
                    {
                        page.FromPosition,
                        page.NextPosition,
                        page.IsEnd
                    })
                    .AddLinks(
                        Links
                            .FromOperation(operation)
                            .Index()
                            .Find()
                            .Browse()
                            .AllStreamNavigation(page, operation))
                    .AddEmbeddedCollection(
                        Constants.Relations.Message,
                        streamMessages.Zip(
                            payloads,
                            (message, payload) => new HALResponse(new
                                {
                                    message.MessageId,
                                    message.CreatedUtc,
                                    message.Position,
                                    message.StreamId,
                                    message.StreamVersion,
                                    message.Type,
                                    payload,
                                    metadata = message.JsonMetadata
                                })
                                .AddLinks(
                                    Links
                                        .FromOperation(operation)
                                        .Add(
                                            Constants.Relations.Message,
                                            $"streams/{message.StreamId}/{message.StreamVersion}",
                                            $"{message.StreamId}@{message.StreamVersion}")
                                        .Self()
                                        .Add(
                                            Constants.Relations.Feed,
                                            $"streams/{message.StreamId}",
                                            message.StreamId)))));

            if(operation.FromPositionInclusive == Position.End)
            {
                var headPosition = streamMessages.Length > 0
                    ? streamMessages[0].Position
                    : Position.End;

                response.Headers[Constants.Headers.HeadPosition] = new[] { $"{headPosition}" };
            }

            if(page.TryGetETag(operation.FromPositionInclusive, out var eTag))
            {
                response.Headers.Add(eTag);
                response.Headers.Add(CacheControl.NoCache);
            }
            else
            {
                response.Headers.Add(CacheControl.OneYear);
            }

            return response;
        }
    }
}