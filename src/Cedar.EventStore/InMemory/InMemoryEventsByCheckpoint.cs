namespace Cedar.EventStore.InMemory
{
    using System.Collections.Generic;

    internal class InMemoryEventsByCheckpoint : Dictionary<long, LinkedListNode<InMemoryStreamEvent>> { }
}