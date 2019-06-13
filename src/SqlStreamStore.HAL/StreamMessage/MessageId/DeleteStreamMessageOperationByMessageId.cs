namespace SqlStreamStore.HAL.StreamMessage
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;

    internal class DeleteStreamMessageOperationByMessageId : IStreamStoreOperation<Unit>
    {
        public DeleteStreamMessageOperationByMessageId(HttpContext context)
        {
            Path = context.Request.Path;

            StreamId = context.GetRouteData().GetStreamId();
            MessageId = context.GetRouteData().GetMessageId();
        }

        public string StreamId { get; }
        public Guid MessageId { get; }
        public PathString Path { get; }

        public async Task<Unit> Invoke(IStreamStore streamStore, CancellationToken ct)
        {
            await streamStore.DeleteMessage(StreamId, MessageId, ct);

            return Unit.Instance;
        }
    }
}