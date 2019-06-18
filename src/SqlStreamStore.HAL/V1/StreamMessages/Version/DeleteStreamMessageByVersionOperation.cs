namespace SqlStreamStore.V1.StreamMessages.Version
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using SqlStreamStore.V1;

    internal class DeleteStreamMessageByVersionOperation : IStreamStoreOperation<Unit>
    {
        public DeleteStreamMessageByVersionOperation(HttpContext context)
        {
            Path = context.Request.Path;

            StreamId = context.GetRouteData().GetStreamId();
            StreamVersion = context.GetRouteData().GetStreamVersion();
        }

        public string StreamId { get; }
        public int StreamVersion { get; }
        public PathString Path { get; }

        public async Task<Unit> Invoke(IStreamStore streamStore, CancellationToken ct)
        {
            var messageId = (await streamStore.ReadStreamBackwards(
                                StreamId,
                                StreamVersion,
                                1,
                                true,
                                ct))
                            .Messages.FirstOrDefault(
                                message => StreamVersion == SqlStreamStore.V1.Streams.StreamVersion.End
                                           || message.StreamVersion == StreamVersion)
                            .MessageId;
            await streamStore.DeleteMessage(StreamId, messageId, ct);

            return Unit.Instance;
        }
    }
}