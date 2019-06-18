namespace SqlStreamStore.V1.StreamMetadata
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using SqlStreamStore.V1;
    using SqlStreamStore.V1.Streams;

    internal class GetStreamMetadataOperation : IStreamStoreOperation<StreamMetadataResult>
    {
        public GetStreamMetadataOperation(HttpContext context)
        {
            Path = context.Request.Path;
            StreamId = context.GetRouteData().GetStreamId();
        }

        public string StreamId { get; }
        public PathString Path { get; }

        public Task<StreamMetadataResult> Invoke(IStreamStore streamStore, CancellationToken ct)
            => streamStore.GetStreamMetadata(StreamId, ct);
    }
}