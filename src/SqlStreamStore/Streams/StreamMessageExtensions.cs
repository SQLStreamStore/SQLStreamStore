namespace SqlStreamStore.Streams
{
    using StreamStoreStore.Json;

    public static class StreamMessageExtensions
    {
        public static T JsonDataAs<T>(this StreamMessage streamMessage)
        {
            return SimpleJson.DeserializeObject<T>(streamMessage.JsonData);
        }

        public static T JsonMetadataAs<T>(this StreamMessage streamMessage)
        {
            return SimpleJson.DeserializeObject<T>(streamMessage.JsonMetadata);
        }
    }
}