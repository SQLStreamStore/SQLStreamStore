namespace SqlStreamStore.Streams
{
    using StreamStoreStore.Json;

    public static class StreamMessageExtensions
    {
        /// <summary>
        ///     Deserializes the json data using the bundled json serializer.
        /// </summary>
        /// <typeparam name="T">The type to deserialize to.</typeparam>
        /// <param name="streamMessage">The stream message that contains the json data.</param>
        /// <returns>The deserialized object.</returns>
        public static T JsonDataAs<T>(this StreamMessage streamMessage)
        {
            return SimpleJson.DeserializeObject<T>(streamMessage.JsonData);
        }


        /// <summary>
        ///     Deserializes the json meta data using the bundled json serializer.
        /// </summary>
        /// <typeparam name="T">The type to deserialize to.</typeparam>
        /// <param name="streamMessage">The stream message that contains the json meta data.</param>
        /// <returns>The deserialized object.</returns>
        public static T JsonMetadataAs<T>(this StreamMessage streamMessage)
        {
            return SimpleJson.DeserializeObject<T>(streamMessage.JsonMetadata);
        }
    }
}