namespace SqlStreamStore.HAL.StreamMetadata
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Streams;

    internal class GetStreamMetadataOperation : IStreamStoreOperation<StreamMetadataResult>
    {
        public GetStreamMetadataOperation(HttpRequest request)
        {
            Path = request.Path;
            StreamId = request.Path.Value.Split('/')[2];
        }

        public string StreamId { get; }
        public PathString Path { get; }

        public Task<StreamMetadataResult> Invoke(IStreamStore streamStore, CancellationToken ct)
            => streamStore.GetStreamMetadata(StreamId, ct);
    }
}