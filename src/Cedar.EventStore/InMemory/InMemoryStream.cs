namespace Cedar.EventStore.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;

    internal class InMemoryStream
    {
        private readonly string _streamId;
        private readonly InMemoryAllStream _inMemoryAllStream;
        private readonly InMemoryEventsByCheckpoint _inMemoryEventsByCheckpoint;
        private readonly GetUtcNow _getUtcNow;
        private readonly List<InMemoryStreamEvent> _events = new List<InMemoryStreamEvent>();
        private readonly HashSet<Guid> _eventIds = new HashSet<Guid>();

        internal InMemoryStream(string streamId,
            InMemoryAllStream inMemoryAllStream,
            InMemoryEventsByCheckpoint inMemoryEventsByCheckpoint,
            GetUtcNow getUtcNow)
        {
            _streamId = streamId;
            _inMemoryAllStream = inMemoryAllStream;
            _inMemoryEventsByCheckpoint = inMemoryEventsByCheckpoint;
            _getUtcNow = getUtcNow;
        }

        internal IReadOnlyList<InMemoryStreamEvent> Events => _events;

        public void AppendToStream(int expectedVersion, NewStreamEvent[] newEvents)
        {
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    AppendToStreamExpectedVersionAny(expectedVersion, newEvents);
                    return;
                case ExpectedVersion.NoStream:
                    AppendToStreamExpectedVersionNoStream(expectedVersion, newEvents);
                    return;
                default:
                    AppendToStreamExpectedVersion(expectedVersion, newEvents);
                    return;
            }
        }

        private void AppendToStreamExpectedVersion(int expectedVersion, NewStreamEvent[] newEvents)
        {
            // Need to do optimistic concurrency check...
            int currentVersion = _events.LastOrDefault()?.StreamVersion ?? 0;
            if(expectedVersion > currentVersion)
            {
                throw new WrongExpectedVersionException(
                    Messages.AppendFailedWrongExpectedVersion.FormatWith(_streamId, expectedVersion));
            }

            if(expectedVersion < currentVersion)
            {
                // expectedVersion < currentVersion, Idempotency test
                for(int i = 0; i < newEvents.Length; i++)
                {
                    int index = expectedVersion + i + 1;
                    if(index >= _events.Count)
                    {
                        throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(_streamId, expectedVersion));
                    }
                    if(_events[index].EventId != newEvents[i].EventId)
                    {
                        throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion.FormatWith(_streamId, expectedVersion));
                    }
                }
                return;
            }

            // expectedVersion == currentVersion)
            if(newEvents.Any(newStreamEvent => _eventIds.Contains(newStreamEvent.EventId)))
            {
                throw new WrongExpectedVersionException(
                    Messages.AppendFailedWrongExpectedVersion.FormatWith(_streamId, expectedVersion));
            }

            AppendEvents(newEvents);
            return;
        }

        private void AppendToStreamExpectedVersionAny(int expectedVersion, NewStreamEvent[] newEvents)
        {
            // idemponcy check - how many newEvents have already been written?
            var newEventIds = new HashSet<Guid>(newEvents.Select(e => e.EventId));
            newEventIds.ExceptWith(_eventIds);

            if(newEventIds.Count == 0)
            {
                // All events have already been written, we're idempotent
                return;
            }

            if(newEventIds.Count != newEvents.Length)
            {
                // Some of the events have already been written, bad request
                throw new WrongExpectedVersionException(
                    Messages.AppendFailedWrongExpectedVersion.FormatWith(_streamId, expectedVersion));
            }

            // None of the events were written previously...
            AppendEvents(newEvents);
        }

        private void AppendToStreamExpectedVersionNoStream(int expectedVersion, NewStreamEvent[] newEvents)
        {
            if(_events.Count > 0)
            {
                //Already committed events, do idempotency check
                if(newEvents.Length > _events.Count)
                {
                    throw new WrongExpectedVersionException(
                        Messages.AppendFailedWrongExpectedVersion.FormatWith(_streamId, expectedVersion));
                }

                if(newEvents.Where((@event, index) => _events[index].EventId != @event.EventId).Any())
                {
                    throw new WrongExpectedVersionException(
                        Messages.AppendFailedWrongExpectedVersion.FormatWith(_streamId, expectedVersion));
                }
                return;
            }

            // None of the events were written previously...
            AppendEvents(newEvents);
        }

        private void AppendEvents(NewStreamEvent[] newEvents)
        {
            long checkPoint = _inMemoryAllStream.Last.Value.Checkpoint;
            int streamRevision = _events.LastOrDefault()?.StreamVersion ?? -1;

            foreach(var newStreamEvent in newEvents)
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
    }
}