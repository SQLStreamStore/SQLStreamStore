namespace SqlStreamStore.V1
{
    using System;
    using Newtonsoft.Json;
    using SqlStreamStore.V1.Streams;

    internal class NewStreamMessageConverter : JsonConverter<NewStreamMessage>
    {
        public override bool CanRead => false;

        public override void WriteJson(JsonWriter writer, NewStreamMessage value, JsonSerializer serializer)
        {
            writer.WriteStartObject();

            if(value.JsonMetadata != null)
            {
                writer.WritePropertyName("jsonMetadata");
                writer.WriteRawValue(value.JsonMetadata);
            }

            writer.WritePropertyName("messageId");
            writer.WriteValue(value.MessageId);

            writer.WritePropertyName("type");
            writer.WriteValue(value.Type);

            writer.WritePropertyName("jsonData");
            writer.WriteRawValue(value.JsonData);


            writer.WriteEndObject();
        }

        public override NewStreamMessage ReadJson(
            JsonReader reader,
            Type objectType,
            NewStreamMessage existingValue,
            bool hasExistingValue,
            JsonSerializer serializer) =>
            throw new NotImplementedException();
    }
}