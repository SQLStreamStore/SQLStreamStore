namespace Cedar.EventStore
{
    using EnsureThat;

    public static class StreamEventExtenions
    {
        public static T JsonDataAs<T>(this StreamEvent streamEvent)
        {
            Ensure.That(streamEvent, "streamEvent").IsNotNull();

            return SimpleJson.DeserializeObject<T>(streamEvent.JsonData);
        }

        public static T JsonMetaDataAs<T>(this StreamEvent streamEvent)
        {
            Ensure.That(streamEvent, "streamEvent").IsNotNull();

            return SimpleJson.DeserializeObject<T>(streamEvent.JsonMetadata);
        }
    }
}