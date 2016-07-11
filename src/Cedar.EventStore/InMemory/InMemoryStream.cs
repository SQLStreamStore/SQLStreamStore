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
        private readonly GetUtcNow _getUtcNow;
        private readonly Action _onStreamAppended;
        private readonly List<InMemoryStreamEvent> _events = new List<InMemoryStreamEvent>();
        private readonly Dictionary<Guid, InMemoryStreamEvent> _eventsById = new Dictionary<Guid, InMemoryStreamEvent>();
        private int _currentVersion = -1;

        internal InMemoryStream(
            string streamId,
            InMemoryAllStream inMemoryAllStream,
            GetUtcNow getUtcNow,
            Action onStreamAppended)
        {
            _streamId = streamId;
            _inMemoryAllStream = inMemoryAllStream;
            _getUtcNow = getUtcNow;
            _onStreamAppended = onStreamAppended;
        }

        internal IReadOnlyList<InMemoryStreamEvent> Events => _events;

        internal void AppendToStream(int expectedVersion, NewStreamEvent[] newEvents)
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
            if(expectedVersion > _currentVersion)
            {
                throw new WrongExpectedVersionException(
                    Messages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
            }

            if(_currentVersion >= 0 && expectedVersion < _currentVersion)
            {
                // expectedVersion < currentVersion, Idempotency test
                for(int i = 0; i < newEvents.Length; i++)
                {
                    int index = expectedVersion + i + 1;
                    if(index >= _events.Count)
                    {
                        throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
                    }
                    if(_events[index].EventId != newEvents[i].EventId)
                    {
                        throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
                    }
                }
                return;
            }

            // expectedVersion == currentVersion)
            if(newEvents.Any(newStreamEvent => _eventsById.ContainsKey(newStreamEvent.EventId)))
            {
                throw new WrongExpectedVersionException(
                    Messages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
            }

            AppendEvents(newEvents);
        }

        private void AppendToStreamExpectedVersionAny(int expectedVersion, NewStreamEvent[] newEvents)
        {
            // idemponcy check - how many newEvents have already been written?
            var newEventIds = new HashSet<Guid>(newEvents.Select(e => e.EventId));
            newEventIds.ExceptWith(_eventsById.Keys);

            if(newEventIds.Count == 0)
            {
                // All events have already been written, we're idempotent
                return;
            }

            if(newEventIds.Count != newEvents.Length)
            {
                // Some of the events have already been written, bad request
                throw new WrongExpectedVersionException(
                    Messages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
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
                        Messages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
                }

                if(newEvents.Where((@event, index) => _events[index].EventId != @event.EventId).Any())
                {
                    throw new WrongExpectedVersionException(
                        Messages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
                }
                return;
            }

            // None of the events were written previously...
            AppendEvents(newEvents);
        }

        private void AppendEvents(NewStreamEvent[] newEvents)
        {
            long checkPoint = _inMemoryAllStream.Last.Value.Checkpoint;

            foreach(var newStreamEvent in newEvents)
            {
                checkPoint++;
                _currentVersion++;

                var inMemoryStreamEvent = new InMemoryStreamEvent(
                    _streamId,
                    newStreamEvent.EventId,
                    _currentVersion,
                    checkPoint,
                    _getUtcNow().UtcDateTime,
                    newStreamEvent.Type,
                    newStreamEvent.JsonData,
                    newStreamEvent.JsonMetadata);

                _inMemoryAllStream.AddAfter(_inMemoryAllStream.Last, inMemoryStreamEvent);
                _events.Add(inMemoryStreamEvent);
                _eventsById.Add(newStreamEvent.EventId, inMemoryStreamEvent);

                _onStreamAppended();
            }
        }

        internal void DeleteAllEvents(int expectedVersion)
        {
            if (expectedVersion > 0 && expectedVersion != _currentVersion)
            {
                throw new WrongExpectedVersionException(
                   Messages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
            }

            foreach (var inMemoryStreamEvent in _events)
            {
                _inMemoryAllStream.Remove(inMemoryStreamEvent);
            }
            _events.Clear();
            _eventsById.Clear();
        }

        public bool DeleteEvent(Guid eventId)
        {
            InMemoryStreamEvent inMemoryStreamEvent;
            if(!_eventsById.TryGetValue(eventId, out inMemoryStreamEvent))
            {
                return false;
            }

            _events.Remove(inMemoryStreamEvent);
            _inMemoryAllStream.Remove(inMemoryStreamEvent);
            _eventsById.Remove(eventId);
            return true;
        }
    }
}