namespace SqlStreamStore.HAL.StreamMessage
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    internal class DeleteStreamMessageOperation : IStreamStoreOperation<Unit>
    {
        public DeleteStreamMessageOperation(HttpRequest request)
        {
            Path = request.Path;
            var pieces = request.Path.Value.Split('/').Reverse().Take(2).ToArray();

            StreamId = pieces.LastOrDefault();

            if(Guid.TryParse(pieces.First(), out var messageId))
            {
                MessageId = messageId;
            }
            else
            {
                StreamVersion = int.Parse(pieces.First());
            }
        }

        public string StreamId { get; }
        public int? StreamVersion { get; }
        public Guid? MessageId { get; }
        public PathString Path { get; }

        public async Task<Unit> Invoke(IStreamStore streamStore, CancellationToken ct)
        {
            var messageId = MessageId ?? (await streamStore.ReadStreamBackwards(
                                StreamId,
                                StreamVersion.GetValueOrDefault(-1),
                                1,
                                true,
                                ct))
                            .Messages.FirstOrDefault(
                                message => StreamVersion == SqlStreamStore.Streams.StreamVersion.End
                                           || message.StreamVersion == StreamVersion)
                            .MessageId;
            await streamStore.DeleteMessage(StreamId, messageId, ct);

            return Unit.Instance;
        }
    }
}