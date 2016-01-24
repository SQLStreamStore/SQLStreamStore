namespace Cedar.EventStore.InMemory
{
    using System.Collections.Concurrent;

    internal class InMemoryStreams : ConcurrentDictionary<string, InMemoryStream> { }
}