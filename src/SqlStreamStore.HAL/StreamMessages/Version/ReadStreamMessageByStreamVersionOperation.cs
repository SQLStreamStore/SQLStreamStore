namespace SqlStreamStore.StreamMessages.Version
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using SqlStreamStore.Streams;

    internal class ReadStreamMessageByStreamVersionOperation : IStreamStoreOperation<StreamMessage>
    {
        public ReadStreamMessageByStreamVersionOperation(HttpContext context)
        {
            var request = context.Request;

            Path = request.Path;

            StreamId = context.GetRouteData().GetStreamId();
            StreamVersion = context.GetRouteData().GetStreamVersion();
        }

        public PathString Path { get; }
        public int StreamVersion { get; }
        public string StreamId { get; }

        public Task<StreamMessage> Invoke(IStreamStore streamStore, CancellationToken ct)
            => streamStore
                .ReadStreamBackwards(StreamId, StreamVersion, 1, true, ct)
                .FirstOrDefaultAsync(message => StreamVersion == SqlStreamStore.Streams.StreamVersion.End
                                                || message.StreamVersion == StreamVersion,
                    ct)
                .AsTask();
    }
}