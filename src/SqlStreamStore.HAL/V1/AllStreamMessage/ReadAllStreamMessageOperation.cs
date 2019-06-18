namespace SqlStreamStore.V1.AllStreamMessage
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using SqlStreamStore.V1;
    using SqlStreamStore.V1.Streams;

    internal class ReadAllStreamMessageOperation : IStreamStoreOperation<StreamMessage>
    {
        public ReadAllStreamMessageOperation(HttpContext context)
        {
            Path = context.Request.Path;
            Position = context.GetRouteData().GetPosition();
        }

        public long Position { get; }
        public PathString Path { get; }

        public async Task<StreamMessage> Invoke(IStreamStore streamStore, CancellationToken ct)
        {
            var page = await streamStore.ReadAllForwards(Position, 1, true, ct);

            return page.Messages.Where(m => m.Position == Position).FirstOrDefault();
        }
    }
}