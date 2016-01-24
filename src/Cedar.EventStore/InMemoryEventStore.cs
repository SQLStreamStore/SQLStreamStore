namespace Cedar.EventStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;

    public sealed class InMemoryEventStore : IEventStore
    {
       

        private readonly GetUtcNow _getUtcNow;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        private readonly LinkedList<InMemoryStreamEvent> _allEvents = new LinkedList<InMemoryStreamEvent>();
        private readonly ConcurrentDictionary<string, List<InMemoryStreamEvent>> _streams
            = new ConcurrentDictionary<string, List<InMemoryStreamEvent>>();
        private readonly Dictionary<long, LinkedListNode<InMemoryStreamEvent>> _eventsByCheckpoint
            = new Dictionary<long, LinkedListNode<InMemoryStreamEvent>>();
        private bool _isDisposed;

        public InMemoryEventStore(GetUtcNow getUtcNow = null)
        {
            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            _allEvents.AddFirst(new InMemoryStreamEvent(
                Guid.NewGuid(),
                -1,
                -1,
                _getUtcNow(),
                null,
                null,
                null));
        }

        public void Dispose()
        {
            if(_isDisposed)
            {
                return;
            }
            _lock.EnterWriteLock();
            try
            {
                _allEvents.Clear();
                _streams.Clear();
                _eventsByCheckpoint.Clear();
                _isDisposed = true;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = new CancellationToken())
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(InMemoryEventStore));

            _lock.EnterWriteLock();
            try
            {
                long checkPoint = _allEvents.Last.Value.Checkpoint;

                var stream = _streams.GetOrAdd(streamId, _ => new List<InMemoryStreamEvent>());
                int streamRevision = stream.LastOrDefault()?.StreamVersion ?? -1;
                
                foreach(var newStreamEvent in events)
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

                    var linkedListNode = _allEvents.AddAfter(_allEvents.Last, inMemoryStreamEvent);
                    stream.Add(inMemoryStreamEvent);
                    _eventsByCheckpoint.Add(checkPoint, linkedListNode);

                }

                return Task.FromResult(0);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new System.NotImplementedException();
        }

        public Task<AllEventsPage> ReadAll(
            string fromCheckpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new System.NotImplementedException();
        }

        public Task<StreamEventsPage> ReadStream(
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = new CancellationToken())
        {
            if(_isDisposed) throw new ObjectDisposedException(nameof(InMemoryEventStore));

            _lock.EnterReadLock();
            try
            {
                List<InMemoryStreamEvent> stream;
                if(!_streams.TryGetValue(streamId, out stream))
                {
                    var notFound = new StreamEventsPage(streamId, PageReadStatus.StreamNotFound, start, -1, -1, direction, true);
                    return Task.FromResult(notFound);
                }

                if(direction == ReadDirection.Forward)
                {
                    var page = ReadStreamForwards(streamId, start, count, stream);

                    return Task.FromResult(page);
                }

                else
                {
                    var page = ReadStreamBackwards(streamId, start, count, stream);
                    return Task.FromResult(page);
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public Task<IStreamSubscription> SubscribeToStream(
            string streamId,
            EventReceived eventReceived,
            SubscriptionDropped subscriptionDropped,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new System.NotImplementedException();
        }

        public string StartCheckpoint => LongCheckpoint.Start.ToString();

        public string EndCheckpoint => LongCheckpoint.End.ToString();

        private static StreamEventsPage ReadStreamForwards(
            string streamId,
            int start,
            int count,
            IReadOnlyList<InMemoryStreamEvent> stream)
        {
            var events = new List<StreamEvent>();
            int i = start;

            while (i < stream.Count && count > 0)
            {
                var inMemoryStreamEvent = stream[i];
                var streamEvent = new StreamEvent(
                    streamId,
                    inMemoryStreamEvent.EventId,
                    inMemoryStreamEvent.StreamVersion,
                    inMemoryStreamEvent.Checkpoint.ToString(),
                    inMemoryStreamEvent.Created,
                    inMemoryStreamEvent.Type,
                    inMemoryStreamEvent.JsonData,
                    inMemoryStreamEvent.JsonMetadata);
                events.Add(streamEvent);

                i++;
                count--;
            }

            int lastStreamVersion = stream.Last().StreamVersion;
            int nextStreamVersion = events.Last().StreamVersion + 1;
            bool endOfStream = i == stream.Count;

            var page = new StreamEventsPage(
                streamId,
                PageReadStatus.Success,
                start,
                nextStreamVersion,
                lastStreamVersion,
                ReadDirection.Forward,
                endOfStream,
                events.ToArray());
            return page;
        }

        private static StreamEventsPage ReadStreamBackwards(
            string streamId,
            int start,
            int count,
            IReadOnlyList<InMemoryStreamEvent> stream)
        {
            var events = new List<StreamEvent>();
            int i = start == StreamPosition.End ? stream.Count - 1 : start;
            while (i >= 0 && count > 0)
            {
                var inMemoryStreamEvent = stream[i];
                var streamEvent = new StreamEvent(
                    streamId,
                    inMemoryStreamEvent.EventId,
                    inMemoryStreamEvent.StreamVersion,
                    inMemoryStreamEvent.Checkpoint.ToString(),
                    inMemoryStreamEvent.Created,
                    inMemoryStreamEvent.Type,
                    inMemoryStreamEvent.JsonData,
                    inMemoryStreamEvent.JsonMetadata);
                events.Add(streamEvent);

                i--;
                count--;
            }

            int lastStreamVersion = stream.Last().StreamVersion;
            int nextStreamVersion = events.Last().StreamVersion - 1;
            bool endOfStream = nextStreamVersion < 0;

            var page = new StreamEventsPage(
                streamId,
                PageReadStatus.Success,
                start,
                nextStreamVersion,
                lastStreamVersion,
                ReadDirection.Backward,
                endOfStream,
                events.ToArray());
            return page;
        }

        private class InMemoryStreamEvent
        {
            internal readonly long Checkpoint;
            public readonly DateTimeOffset Created;
            public readonly Guid EventId;
            public readonly string JsonData;
            public readonly string JsonMetadata;
            public readonly int StreamVersion;
            public readonly string Type;

            public InMemoryStreamEvent(
                Guid eventId,
                int streamVersion,
                long checkpoint,
                DateTimeOffset created,
                string type,
                string jsonData,
                string jsonMetadata)
            {
                EventId = eventId;
                StreamVersion = streamVersion;
                Checkpoint = checkpoint;
                Created = created;
                Type = type;
                JsonData = jsonData;
                JsonMetadata = jsonMetadata;
            }
        }
    }
}