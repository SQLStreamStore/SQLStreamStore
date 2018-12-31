namespace SqlStreamStore.HAL.AllStreamMessage
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Streams;

    internal class ReadAllStreamMessageOperation : IStreamStoreOperation<StreamMessage>
    {
        public ReadAllStreamMessageOperation(HttpRequest request)
        {
            Path = request.Path;
            Position = long.Parse(request.Path.Value.Remove(0, 2 + Constants.Streams.All.Length));
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