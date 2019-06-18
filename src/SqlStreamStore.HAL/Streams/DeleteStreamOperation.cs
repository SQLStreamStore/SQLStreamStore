namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;

    internal class DeleteStreamOperation : IStreamStoreOperation<Unit>
    {
        public DeleteStreamOperation(HttpContext context)
        {
            Path = context.Request.Path;
            StreamId = context.GetRouteData().GetStreamId();
            ExpectedVersion = context.Request.GetExpectedVersion();
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
