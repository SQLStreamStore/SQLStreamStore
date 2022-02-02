namespace SqlStreamStore.HAL.StreamMessage.MessageId
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using SqlStreamStore.Streams;

    internal class DeleteStreamMessageByMessageIdOperation : IStreamStoreOperation<Unit>
    {
        public DeleteStreamMessageByMessageIdOperation(HttpContext context)
        {
            Path = context.Request.Path;

            StreamId = context.GetRouteData().GetStreamId();
            MessageId = context.GetRouteData().GetMessageId();
        }

        public string StreamId { get; }
        public Guid MessageId { get; }
        public PathString Path { get; }

        public async Task<Unit> Invoke(IStreamStore<ReadAllPage> streamStore, CancellationToken ct)
        {
            await streamStore.DeleteMessage(StreamId, MessageId, ct);

            return Unit.Instance;
        }
    }
}