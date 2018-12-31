namespace SqlStreamStore.HAL.Streams
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    internal class DeleteStreamOperation : IStreamStoreOperation<Unit>
    {
        public DeleteStreamOperation(HttpRequest request)
        {
            Path = request.Path;
            StreamId = request.Path.Value.Remove(0, 2 + Constants.Streams.Stream.Length);
            ExpectedVersion = request.GetExpectedVersion();
        }

        public string StreamId { get; }
        public int ExpectedVersion { get; }
        public PathString Path { get; }

        public async Task<Unit> Invoke(IStreamStore streamStore, CancellationToken ct)
        {
            await streamStore.DeleteStream(StreamId, ExpectedVersion, ct);

            return Unit.Instance;
        }
    }
}
