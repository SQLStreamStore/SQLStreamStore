namespace SqlStreamStore.HAL.StreamMetadata
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Halcyon.HAL;
    using Newtonsoft.Json.Linq;

    internal class StreamMetadataResource : IResource
    {
        private readonly IStreamStore _streamStore;
        public SchemaSet Schema { get; }

        public StreamMetadataResource(IStreamStore streamStore)
        {
            if(streamStore == null)
                throw new ArgumentNullException(nameof(streamStore));
            _streamStore = streamStore;
            Schema = new SchemaSet<StreamMetadataResource>();
        }

        private HALResponse metadata => Schema.GetSchema(nameof(metadata));

        public async Task<Response> Get(
            GetStreamMetadataOperation operation,
            CancellationToken cancellationToken)
        {
            var result = await operation.Invoke(_streamStore, cancellationToken);

            var response = new HalJsonResponse(new HALResponse(new
                    {
                        result.StreamId,
                        result.MetadataStreamVersion,
                        result.MaxAge,
                        result.MaxCount,
                        MetadataJson = string.IsNullOrEmpty(result.MetadataJson)
                            ? default
                            : JObject.Parse(result.MetadataJson)
                    })
                    .AddLinks(
                        Links
                            .FromOperation(operation)
                            .Index()
                            .Find()
                            .Browse()
                            .StreamMetadataNavigation(operation))
                    .AddEmbeddedResource(
                        Constants.Relations.Metadata,
                        metadata),
                result.MetadataStreamVersion >= 0 ? 200 : 404)
            {
                Headers =
                {
                    ETag.FromStreamVersion(result.MetadataStreamVersion)
                }
            };

            return response;
        }

        public async Task<Response> Post(
            SetStreamMetadataOperation operation,
            CancellationToken cancellationToken)
        {
            await operation.Invoke(_streamStore, cancellationToken);

            var response = new HalJsonResponse(new HALResponse(new
                {
                    operation.StreamId,
                    operation.MaxAge,
                    operation.MaxCount,
                    operation.MetadataJson
                })
                .AddLinks(
                    Links
                        .FromOperation(operation)
                        .Index()
                        .Find()
                        .StreamMetadataNavigation(operation)));

            return response;
        }
    }
}