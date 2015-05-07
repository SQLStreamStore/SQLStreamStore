namespace Cedar.EventStore
{
    using EnsureThat;

    public static class StreamEventExtenions
    {
        public static T JsonDataAs<T>(this StreamEvent streamEvent, IJsonSerializer jsonSerializer = null)
        {
            Ensure.That(streamEvent, "streamEvent").IsNotNull();

            jsonSerializer = jsonSerializer ?? DefaultJsonSerializer.Instance;

            return jsonSerializer.Deserialize<T>(streamEvent.JsonData);
        }

        public static T JsonMetaDataAs<T>(this StreamEvent streamEvent, IJsonSerializer jsonSerializer = null)
        {
            Ensure.That(streamEvent, "streamEvent").IsNotNull();

            jsonSerializer = jsonSerializer ?? DefaultJsonSerializer.Instance;

            return jsonSerializer.Deserialize<T>(streamEvent.JsonMetadata);
        }
    }
}