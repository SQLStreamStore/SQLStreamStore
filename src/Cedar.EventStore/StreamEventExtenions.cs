namespace Cedar.EventStore
{
    using EnsureThat;

    public static class StreamEventExtenions
    {
        public static T JsonAs<T>(this StreamEvent streamEvent, IJsonSerializer jsonSerializer = null)
        {
            Ensure.That(streamEvent, "streamEvent").IsNotNull();

            jsonSerializer = jsonSerializer ?? DefaultJsonSerializer.Instance;

            return jsonSerializer.Deserialize<T>(streamEvent.Json);
        }
    }
}