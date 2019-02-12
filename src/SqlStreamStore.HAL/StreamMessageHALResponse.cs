namespace SqlStreamStore.HAL
{
    using Halcyon.HAL;
    using Newtonsoft.Json.Linq;

    internal class StreamMessageHALResponse : HALResponse
    {
        public StreamMessageHALResponse(SqlStreamStore.Streams.StreamMessage message, string payload)
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

        private static JObject FromString(string data) => data == default ? default : JObject.Parse(data);
    }
}