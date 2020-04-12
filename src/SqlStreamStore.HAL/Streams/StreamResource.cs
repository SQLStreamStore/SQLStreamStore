namespace SqlStreamStore.HAL.Streams
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Halcyon.HAL;
    using SqlStreamStore.Streams;

    internal class StreamResource : IResource
    {
        private readonly IStreamStore _streamStore;
        private readonly string _relativePathToRoot;
        private readonly bool _useCanonicalUrls;
        public SchemaSet Schema { get; }

        public StreamResource(IStreamStore streamStore, bool useCanonicalUrls)
        {
            if(streamStore == null)
                throw new ArgumentNullException(nameof(streamStore));
            _streamStore = streamStore;
            _relativePathToRoot = "../";
            _useCanonicalUrls = useCanonicalUrls;
            Schema = new SchemaSet<StreamResource>();
        }

        private HALResponse append => Schema.GetSchema(nameof(append));
        private HALResponse delete => Schema.GetSchema("delete-stream");

        public async Task<Response> Post(
            AppendStreamOperation operation,
            CancellationToken cancellationToken)
        {
            if(operation.ExpectedVersion < Constants.Headers.MinimumExpectedVersion)
            {
                return new HalJsonResponse(new HALResponse(new
                    {
                        type = typeof(WrongExpectedVersionException).Name,
                        title = "Wrong expected version.",
                        detail =
                            $"Expected header '{Constants.Headers.ExpectedVersion}' to have an expected version => {ExpectedVersion.NoStream}."
                    }),
                    400);
            }

            var result = await operation.Invoke(_streamStore, cancellationToken);

            var links = Links
                .FromOperation(operation)
                .Index()
                .Find()
                .Browse()
                .Add(Constants.Relations.Feed, LinkFormatter.Stream(operation.StreamId)).Self();

            var response = new HalJsonResponse(
                new HALResponse(result)
                    .AddLinks(links),
                operation.ExpectedVersion == ExpectedVersion.NoStream
                    ? 201
                    : 200);
            if(response.StatusCode == 201)
            {
                response.Headers[Constants.Headers.Location] =
                    new[] { $"{_relativePathToRoot}{LinkFormatter.Stream(operation.StreamId)}" };
            }

            return response;
        }

        public async Task<Response> Get(ReadStreamOperation operation, CancellationToken cancellationToken)
        {
            if(_useCanonicalUrls && !operation.IsUriCanonical)
            {
                return new PermanentRedirectResponse($"../{operation.Self}");
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
                        page.LastStreamVersion,
                        page.LastStreamPosition,
                        page.FromStreamVersion,
                        page.NextStreamVersion,
                        page.IsEnd
                    })
                    .AddLinks(Links
                        .FromOperation(operation)
                        .Index()
                        .Find()
                        .Browse()
                        .StreamsNavigation(page, operation))
                    .AddEmbeddedResource(
                        Constants.Relations.AppendToStream,
                        append)
                    .AddEmbeddedResource(
                        Constants.Relations.DeleteStream,
                        delete)
                    .AddEmbeddedCollection(
                        Constants.Relations.Message,
                        streamMessages.Zip(
                            payloads,
                            (message, payload) => new StreamMessageHALResponse(message, payload)
                                .AddLinks(
                                    Links
                                        .FromOperation(operation)
                                        .Add(
                                            Constants.Relations.Message,
                                            LinkFormatter.StreamMessageByStreamVersion(
                                                message.StreamId,
                                                message.StreamVersion),
                                            $"{message.StreamId}@{message.StreamVersion}")
                                        .Self()
                                        .Add(
                                            Constants.Relations.Feed,
                                            LinkFormatter.Stream(message.StreamId),
                                            message.StreamId)))),
                page.Status == PageReadStatus.StreamNotFound ? 404 : 200);

            if(page.TryGetETag(out var eTag))
            {
                response.Headers.Add(eTag);
            }

            return response;
        }

        public async Task<Response> Delete(DeleteStreamOperation operation, CancellationToken cancellationToken)
        {
            await operation.Invoke(_streamStore, cancellationToken);

            return NoContentResponse.Instance;
        }
    }
}