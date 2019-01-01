namespace SqlStreamStore.HAL.StreamMessage
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Streams;

    internal class ReadStreamMessageByStreamVersionOperation : IStreamStoreOperation<StreamMessage>
    {
        public ReadStreamMessageByStreamVersionOperation(HttpRequest request)
        {
            Path = request.Path;
            
            var pieces = request.Path.Value.Split('/').Reverse().Take(2).ToArray();

            StreamId = pieces.LastOrDefault();

            StreamVersion = int.Parse(pieces.First());
        }

        public PathString Path { get; }
        public int StreamVersion { get; }
        public string StreamId { get; }

        public async Task<StreamMessage> Invoke(IStreamStore streamStore, CancellationToken ct)
            => (await streamStore.ReadStreamBackwards(StreamId, StreamVersion, 1, true, ct))
                .Messages.FirstOrDefault(message => StreamVersion == SqlStreamStore.Streams.StreamVersion.End
                                                    || message.StreamVersion == StreamVersion);
    }
}