namespace SqlStreamStore.V1
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;
    using SqlStreamStore.V1.Streams;

    internal class HttpStreamMessage
    {
        public Guid MessageId { get; set; }
        public DateTimeOffset CreatedUtc { get; set; }
        public long Position { get; set; }
        public string StreamId { get; set; }
        public int StreamVersion { get; set; }
        public string Type { get; set; }
        public JObject Payload { get; set; }
        public JObject Metadata { get; set; }

        public StreamMessage ToStreamMessage(Func<CancellationToken, Task<string>> getPayload)
            => new StreamMessage(
                StreamId,
                MessageId,
                StreamVersion,
                Position,
                CreatedUtc.DateTime,
                Type,
                Metadata?.ToString(),
                getPayload);
    }
}