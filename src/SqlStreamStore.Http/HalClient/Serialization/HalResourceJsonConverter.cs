namespace SqlStreamStore.HalClient.Serialization
{
    using System;
    using System.Reflection;
    using Newtonsoft.Json;
    using SqlStreamStore.HalClient.Models;

    internal sealed class HalResourceJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {

        }

        public override object ReadJson(JsonReader reader, Type objectType, object existing, JsonSerializer serializer)
            => HalResourceJsonReader.ReadResource(reader);

        public override bool CanConvert(Type objectType) => typeof (IResource).GetTypeInfo().IsAssignableFrom(objectType.GetTypeInfo());
    }
}