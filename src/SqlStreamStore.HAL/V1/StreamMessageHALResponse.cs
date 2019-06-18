namespace SqlStreamStore.V1
{
    using Halcyon.HAL;
    using Newtonsoft.Json.Linq;

    internal class StreamMessageHALResponse : HALResponse
    {
        public StreamMessageHALResponse(SqlStreamStore.V1.Streams.StreamMessage message, string payload)
            : base(new
            {
                message.MessageId,
                message.CreatedUtc,
                message.Position,
                message.StreamId,
                message.StreamVersion,
                message.Type,
                payload = FromString(payload),
                metadata = FromString(message.JsonMetadata)
            })
        { }

        private static JObject FromString(string data) => string.IsNullOrEmpty(data) ? default : JObject.Parse(data);
    }
}