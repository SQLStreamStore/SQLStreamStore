namespace SqlStreamStore.HAL.StreamMetadata
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using SqlStreamStore.Streams;

    internal class GetStreamMetadataOperation : IStreamStoreOperation<StreamMetadataResult>
    {
        public GetStreamMetadataOperation(HttpContext context)
        {
            Path = context.Request.Path;
            StreamId = context.GetRouteData().GetStreamId();
        }

        public string StreamId { get; }
        public PathString Path { get; }

        public Task<StreamMetadataResult> Invoke(IStreamStore<ReadAllPage> streamStore, CancellationToken ct)
            => streamStore.GetStreamMetadata(StreamId, ct);
    }
}