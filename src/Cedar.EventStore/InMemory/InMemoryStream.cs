namespace Cedar.EventStore.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;

    internal class InMemoryStream
    {
        private readonly InMemoryAllStream _inMemoryAllStream;
        private readonly InMemoryEventsByCheckpoint _inMemoryEventsByCheckpoint;
        private readonly GetUtcNow _getUtcNow;
        private readonly List<InMemoryStreamEvent> _events = new List<InMemoryStreamEvent>();
        private readonly HashSet<Guid> _eventIds = new HashSet<Guid>();

        internal InMemoryStream(InMemoryAllStream inMemoryAllStream, InMemoryEventsByCheckpoint inMemoryEventsByCheckpoint, GetUtcNow getUtcNow)
        {
            _inMemoryAllStream = inMemoryAllStream;
            _inMemoryEventsByCheckpoint = inMemoryEventsByCheckpoint;
            _getUtcNow = getUtcNow;
        }

        public void AppendToStream(NewStreamEvent[] events)
        {
            long checkPoint = _inMemoryAllStream.Last.Value.Checkpoint;

            int streamRevision = _events.LastOrDefault()?.StreamVersion ?? -1;

            foreach (var newStreamEvent in events)
            {
                checkPoint++;
                streamRevision++;

                var inMemoryStreamEvent = new InMemoryStreamEvent(
                    newStreamEvent.EventId,
                    streamRevision,
                    checkPoint,
                    _getUtcNow(),
                    newStreamEvent.Type,
                    newStreamEvent.JsonData,
                    newStreamEvent.JsonMetadata);

                var linkedListNode = _inMemoryAllStream.AddAfter(_inMemoryAllStream.Last, inMemoryStreamEvent);
                _events.Add(inMemoryStreamEvent);
                _inMemoryEventsByCheckpoint.Add(checkPoint, linkedListNode);
                _eventIds.Add(newStreamEvent.EventId);
            }
        }

        internal IReadOnlyList<InMemoryStreamEvent> Events => _events;
    }
}