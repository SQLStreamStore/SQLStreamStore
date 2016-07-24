namespace SqlStreamStore.InMemory
{
    using System.Collections.Generic;

    internal class InMemoryAllStream : LinkedList<InMemoryStreamEvent>
    {}
}