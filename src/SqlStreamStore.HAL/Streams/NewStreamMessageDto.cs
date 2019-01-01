namespace SqlStreamStore.HAL.Streams
{
    using System;
    using Newtonsoft.Json.Linq;
    using SqlStreamStore.Streams;

    internal class NewStreamMessageDto
    {
        public Guid MessageId { get; set; }
        public string Type { get; set; }
        public JToken JsonData { get; set; }
        public JToken JsonMetadata { get; set; }

        public NewStreamMessage ToNewStreamMessage()
            => new NewStreamMessage(MessageId, Type, JsonData.ToString(), JsonMetadata?.ToString());
    }
}