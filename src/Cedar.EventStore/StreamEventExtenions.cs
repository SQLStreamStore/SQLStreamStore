namespace Cedar.EventStore
{
    using EnsureThat;

    public static class StreamEventExtenions
    {
        public static T JsonAs<T>(this StreamEvent streamEvent, ISerializer serializer = null)
        {
            Ensure.That(streamEvent, "streamEvent").IsNotNull();

            serializer = serializer ?? DefaultJsonSerializer.Instance;

            return serializer.Deserialize<T>(streamEvent.Json);
        }
    }
}