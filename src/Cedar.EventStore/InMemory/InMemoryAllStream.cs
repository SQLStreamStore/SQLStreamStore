namespace Cedar.EventStore.InMemory
{
    using System.Collections.Generic;

    internal class InMemoryAllStream : LinkedList<InMemoryStreamEvent>
    {}
}