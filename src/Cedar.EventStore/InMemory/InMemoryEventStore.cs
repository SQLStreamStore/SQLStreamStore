 // ReSharper disable once CheckNamespace
namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.InMemory;
    using Cedar.EventStore.Streams;

    public sealed class InMemoryEventStore : IEventStore
    {
        private readonly GetUtcNow _getUtcNow;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        private readonly InMemoryAllStream _allStream = new InMemoryAllStream();
        private readonly InMemoryStreams _streams = new InMemoryStreams();
        private readonly InMemoryEventsByCheckpoint _eventsByCheckpoint = new InMemoryEventsByCheckpoint();
        private bool _isDisposed;

        public InMemoryEventStore(GetUtcNow getUtcNow = null)
        {
            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            _allStream.AddFirst(new InMemoryStreamEvent(
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
                _allStream.Clear();
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
                if(expectedVersion == ExpectedVersion.NoStream && _streams.ContainsKey(streamId))
                {
                    throw new WrongExpectedVersionException(
                        Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, ExpectedVersion.NoStream));
                }

                var stream = _streams.GetOrAdd(streamId, _ => new InMemoryStream(_allStream, _eventsByCheckpoint, _getUtcNow));
                stream.AppendToStream(events);
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
                InMemoryStream stream;
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
            InMemoryStream stream)
        {
            var events = new List<StreamEvent>();
            int i = start;

            while (i < stream.Events.Count && count > 0)
            {
                var inMemoryStreamEvent = stream.Events[i];
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

            int lastStreamVersion = stream.Events.Last().StreamVersion;
            int nextStreamVersion = events.Last().StreamVersion + 1;
            bool endOfStream = i == stream.Events.Count;

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
            InMemoryStream stream)
        {
            var events = new List<StreamEvent>();
            int i = start == StreamPosition.End ? stream.Events.Count - 1 : start;
            while (i >= 0 && count > 0)
            {
                var inMemoryStreamEvent = stream.Events[i];
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

            int lastStreamVersion = stream.Events.Last().StreamVersion;
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
    }
}