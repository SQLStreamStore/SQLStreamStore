namespace Cedar.EventStore
{
    using EnsureThat;

    public static class StreamEventExtenions
    {
        public static T JsonAs<T>(this StreamEvent streamEvent)
        {
            Ensure.That(streamEvent, "streamEvent").IsNotNull();

            return DefaultJsonSerializer.Deserialize<T>(streamEvent.Json);
        }
    }
}