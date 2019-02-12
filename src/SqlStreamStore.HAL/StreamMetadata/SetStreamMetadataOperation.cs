namespace SqlStreamStore.HAL.StreamMetadata
{
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal class SetStreamMetadataOperation : IStreamStoreOperation<Unit>
    {
        public static async Task<SetStreamMetadataOperation> Create(HttpRequest request, CancellationToken ct)
        {
            using(var reader = new JsonTextReader(new StreamReader(request.Body))
            {
                CloseInput = false
            })
            {
                var body = await JObject.LoadAsync(reader, ct);
                
                return new SetStreamMetadataOperation(request, body);
            }
        }

        private SetStreamMetadataOperation(HttpRequest request, JObject body)
        {
            Path = request.Path;
            StreamId = request.Path.Value.Split('/')[2];
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