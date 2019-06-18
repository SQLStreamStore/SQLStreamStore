namespace SqlStreamStore.V1.StreamMetadata
{
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using SqlStreamStore.V1;

    internal class SetStreamMetadataOperation : IStreamStoreOperation<Unit>
    {
        public static async Task<SetStreamMetadataOperation> Create(HttpContext context)
        {
            using(var reader = new JsonTextReader(new StreamReader(context.Request.Body))
            {
                CloseInput = false
            })
            {
                var body = await JObject.LoadAsync(reader, context.RequestAborted);

                return new SetStreamMetadataOperation(context, body);
            }
        }

        private SetStreamMetadataOperation(HttpContext context, JObject body)
        {
            var request = context.Request;
            Path = request.Path;
            StreamId = context.GetRouteData().GetStreamId();
            ExpectedVersion = request.GetExpectedVersion();
            MaxAge = body.Value<int?>("maxAge");
            MaxCount = body.Value<int?>("maxCount");
            MetadataJson = body.Value<JObject>("metadataJson");
        }

        public PathString Path { get; }
        public string StreamId { get; }
        public int ExpectedVersion { get; }
        public JObject MetadataJson { get; }
        public int? MaxCount { get; }
        public int? MaxAge { get; }

        public async Task<Unit> Invoke(IStreamStore streamStore, CancellationToken ct)
        {
            await streamStore.SetStreamMetadata(
                StreamId,
                ExpectedVersion,
                MaxAge,
                MaxCount,
                MetadataJson?.ToString(Formatting.Indented),
                ct);

            return Unit.Instance;
        }
    }
}