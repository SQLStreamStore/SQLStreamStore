namespace SqlStreamStore.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    internal class InMemoryStream
    {
        private readonly string _streamId;
        private readonly InMemoryAllStream _inMemoryAllStream;
        private readonly GetUtcNow _getUtcNow;
        private readonly Action _onStreamAppended;
        private readonly Func<int> _getNextPosition;
        private readonly List<InMemoryStreamMessage> _events = new List<InMemoryStreamMessage>();
        private readonly Dictionary<Guid, InMemoryStreamMessage> _eventsById = new Dictionary<Guid, InMemoryStreamMessage>();

        internal InMemoryStream(
            string streamId,
            InMemoryAllStream inMemoryAllStream,
            GetUtcNow getUtcNow,
            Action onStreamAppended,
            Func<int> getNextPosition)
        {
            _streamId = streamId;
            _inMemoryAllStream = inMemoryAllStream;
            _getUtcNow = getUtcNow;
            _onStreamAppended = onStreamAppended;
            _getNextPosition = getNextPosition;
        }

        internal IReadOnlyList<InMemoryStreamMessage> Events => _events;

        internal int CurrentVersion { get; private set; } = -1;

        internal void AppendToStream(int expectedVersion, NewStreamMessage[] newMessages)
        {
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    AppendToStreamExpectedVersionAny(expectedVersion, newMessages);
                    return;
                case ExpectedVersion.NoStream:
                    AppendToStreamExpectedVersionNoStream(expectedVersion, newMessages);
                    return;
                default:
                    AppendToStreamExpectedVersion(expectedVersion, newMessages);
                    return;
            }
        }

        private void AppendToStreamExpectedVersion(int expectedVersion, NewStreamMessage[] newMessages)
        {
            // Need to do optimistic concurrency check...
            if(expectedVersion > CurrentVersion)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
            }

            if(CurrentVersion >= 0 && expectedVersion < CurrentVersion)
            {
                // expectedVersion < currentVersion, Idempotency test
                for(int i = 0; i < newMessages.Length; i++)
                {
                    int index = expectedVersion + i + 1;
                    if(index >= _events.Count)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
                    }
                    if(_events[index].MessageId != newMessages[i].MessageId)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
                    }
                }
                return;
            }

            // expectedVersion == currentVersion)
            if(newMessages.Any(newmessage => _eventsById.ContainsKey(newmessage.MessageId)))
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
            }

            AppendEvents(newMessages);
        }

        private void AppendToStreamExpectedVersionAny(int expectedVersion, NewStreamMessage[] newMessages)
        {
            // idemponcy check - how many newMessages have already been written?
            var newEventIds = new HashSet<Guid>(newMessages.Select(e => e.MessageId));
            newEventIds.ExceptWith(_eventsById.Keys);

            if(newEventIds.Count == 0)
            {
                // All Messages have already been written, we're idempotent
                return;
            }

            if(newEventIds.Count != newMessages.Length)
            {
                // Some of the Messages have already been written, bad request
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
            }

            // None of the Messages were written previously...
            AppendEvents(newMessages);
        }

        private void AppendToStreamExpectedVersionNoStream(int expectedVersion, NewStreamMessage[] newMessages)
        {
            if(_events.Count > 0)
            {
                //Already committed Messages, do idempotency check
                if(newMessages.Length > _events.Count)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
                }

                if(newMessages.Where((message, index) => _events[index].MessageId != message.MessageId).Any())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
                }
                return;
            }

            // None of the Messages were written previously...
            AppendEvents(newMessages);
        }

        private void AppendEvents(NewStreamMessage[] newMessages)
        {
            foreach(var newmessage in newMessages)
            {
                var position = _getNextPosition();
                CurrentVersion++;

                var inMemorymessage = new InMemoryStreamMessage(
                    _streamId,
                    newmessage.MessageId,
                    CurrentVersion,
                    position,
                    _getUtcNow(),
                    newmessage.Type,
                    newmessage.JsonData,
                    newmessage.JsonMetadata);

                _events.Add(inMemorymessage);
                _eventsById.Add(newmessage.MessageId, inMemorymessage);
                _inMemoryAllStream.AddAfter(_inMemoryAllStream.Last, inMemorymessage);
            }
            _onStreamAppended();
        }

        internal void DeleteAllEvents(int expectedVersion)
        {
            if (expectedVersion > 0 && expectedVersion != CurrentVersion)
            {
                throw new WrongExpectedVersionException(
                   ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion));
            }

            foreach (var inMemorymessage in _events)
            {
                _inMemoryAllStream.Remove(inMemorymessage);
            }
            _events.Clear();
            _eventsById.Clear();
        }

        public bool DeleteEvent(Guid eventId)
        {
            InMemoryStreamMessage inMemoryStreamMessage;
            if(!_eventsById.TryGetValue(eventId, out inMemoryStreamMessage))
            {
                return false;
            }

            _events.Remove(inMemoryStreamMessage);
            _inMemoryAllStream.Remove(inMemoryStreamMessage);
            _eventsById.Remove(eventId);
            return true;
        }
    }
}