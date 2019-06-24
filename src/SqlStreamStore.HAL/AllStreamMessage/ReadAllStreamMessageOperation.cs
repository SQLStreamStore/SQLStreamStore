namespace SqlStreamStore.AllStreamMessage
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using SqlStreamStore.Streams;

    internal class ReadAllStreamMessageOperation : IStreamStoreOperation<StreamMessage>
    {
        public ReadAllStreamMessageOperation(HttpContext context)
        {
            Path = context.Request.Path;
            Position = context.GetRouteData().GetPosition();
        }

        public long Position { get; }
        public PathString Path { get; }

        public Task<StreamMessage> Invoke(IStreamStore streamStore, CancellationToken ct)
            => streamStore.ReadAllForwards(Position, 1, true, ct)
                .Where(m => m.Position == Position)
                .FirstOrDefaultAsync(ct)
                .AsTask();
    }
}