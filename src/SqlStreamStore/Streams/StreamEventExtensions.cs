namespace SqlStreamStore.Streams
{
    using StreamStoreStore.Json;

    public static class StreamEventExtensions
    {
        public static T JsonDataAs<T>(this StreamEvent streamEvent)
        {
            return SimpleJson.DeserializeObject<T>(streamEvent.JsonData);
        }

        public static T JsonMetadataAs<T>(this StreamEvent streamEvent)
        {
            return SimpleJson.DeserializeObject<T>(streamEvent.JsonMetadata);
        }
    }
}