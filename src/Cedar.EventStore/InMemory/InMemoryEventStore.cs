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
    using Cedar.EventStore.Subscriptions;

    public sealed class InMemoryEventStore : EventStoreBase
    {
        private readonly GetUtcNow _getUtcNow;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        private readonly InMemoryAllStream _allStream = new InMemoryAllStream();
        private readonly InMemoryStreams _streams = new InMemoryStreams();
        private bool _isDisposed;
        private readonly Subject<Unit> _subscriptions = new Subject<Unit>();
        private readonly Action _onStreamAppended;

        public InMemoryEventStore(GetUtcNow getUtcNow = null)
        {
            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            _allStream.AddFirst(new InMemoryStreamEvent(
                "<in-memory-root-event>",
                Guid.NewGuid(),
                -1,
                -1,
                _getUtcNow(),
                null,
                null,
                null));

            _onStreamAppended = () => _subscriptions.OnNext(Unit.Default);
        }

        public override void Dispose()
        {
            if(_isDisposed)
            {
                return;
            }
            _lock.EnterWriteLock();
            try
            {
                _subscriptions.OnCompleted();
                _allStream.Clear();
                _streams.Clear();
                _isDisposed = true;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        protected override Task AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = new CancellationToken())
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(InMemoryEventStore));

            _lock.EnterWriteLock();
            try
            {
                InMemoryStream inMemoryStream;
                // Should only add stream if NoStream
                if (expectedVersion == ExpectedVersion.NoStream || expectedVersion == ExpectedVersion.Any)
                {
                    if(_streams.TryGetValue(streamId, out inMemoryStream))
                    {
                        inMemoryStream.AppendToStream(expectedVersion, events);
                    }
                    else
                    {
                        inMemoryStream = new InMemoryStream(
                            streamId,
                            _allStream,
                            _getUtcNow,
                            _onStreamAppended);
                        inMemoryStream.AppendToStream(expectedVersion, events);
                        _streams.TryAdd(streamId, inMemoryStream);
                    }
                    return Task.FromResult(0);
                }

                if (!_streams.TryGetValue(streamId, out inMemoryStream))
                {
                    throw new WrongExpectedVersionException(
                        Messages.AppendFailedWrongExpectedVersion(streamId, expectedVersion));
                }
                inMemoryStream.AppendToStream(expectedVersion, events);

                return Task.FromResult(0);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = new CancellationToken())
        {
            if(_isDisposed)
                throw new ObjectDisposedException(nameof(InMemoryEventStore));

            _lock.EnterWriteLock();
            try
            {
                if(!_streams.ContainsKey(streamId))
                {
                    if(expectedVersion >= 0)
                    {
                        throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion(streamId, expectedVersion));
                    }
                    return Task.FromResult(0);
                }
                _streams[streamId].Delete(expectedVersion);
                return Task.FromResult(0);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        protected override Task<AllEventsPage> ReadAllInternal(
            long fromCheckpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if(_isDisposed) throw new ObjectDisposedException(nameof(InMemoryEventStore));

            _lock.EnterReadLock();
            try
            {
                var start = fromCheckpoint;
                if(start == Checkpoint.End)
                {
                    start = long.MaxValue;
                }

                // Find the node to start from (it may not be equal to the exact checkpoint)
                var current = _allStream.First;
                if(current.Next == null) //Empty store
                {
                    var page = new AllEventsPage(fromCheckpoint, Checkpoint.Start, true, direction);
                    return Task.FromResult(page);
                }
                LinkedListNode<InMemoryStreamEvent> previous = current.Previous;
                while ( current.Value.Checkpoint < start)
                {
                    if(current.Next == null)
                    {
                        break;
                    }
                    previous = current;
                    current = current.Next;
                }
                if(direction == ReadDirection.Forward)
                {
                    var page = ReadAllForwards(fromCheckpoint, maxCount, direction, current, previous);
                    return Task.FromResult(page);
                }
                else
                {
                    var page = ReadAllBackwards(fromCheckpoint, maxCount, direction, current, previous);
                    return Task.FromResult(page);
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        private AllEventsPage ReadAllBackwards(
            long fromCheckpoint,
            int maxCount,
            ReadDirection direction,
            LinkedListNode<InMemoryStreamEvent> current,
            LinkedListNode<InMemoryStreamEvent> previous)
        {
            var streamEvents = new List<StreamEvent>();
            while(maxCount > 0 && current != _allStream.First)
            {
                var streamEvent = new StreamEvent(
                    current.Value.StreamId,
                    current.Value.EventId,
                    current.Value.StreamVersion,
                    current.Value.Checkpoint,
                    current.Value.Created,
                    current.Value.Type,
                    current.Value.JsonData,
                    current.Value.JsonMetadata);
                streamEvents.Add(streamEvent);
                maxCount--;
                previous = current;
                current = current.Previous;
            }

            bool isEnd;
            if(previous == null || previous.Value.Checkpoint == 0)
            {
                isEnd = true;
            }
            else
            {
                isEnd = false;
            }
            var nextCheckPoint = isEnd
                ? 0
                : current.Value.Checkpoint;

            var page = new AllEventsPage(
                fromCheckpoint,
                nextCheckPoint,
                isEnd,
                direction,
                streamEvents.ToArray());
            return page;
        }

        private static AllEventsPage ReadAllForwards(
            long fromCheckpoint,
            int maxCount,
            ReadDirection direction,
            LinkedListNode<InMemoryStreamEvent> current,
            LinkedListNode<InMemoryStreamEvent> previous)
        {
            var streamEvents = new List<StreamEvent>();
            while(maxCount >= 0 && current != null)
            {
                var streamEvent = new StreamEvent(
                    current.Value.StreamId,
                    current.Value.EventId,
                    current.Value.StreamVersion,
                    current.Value.Checkpoint,
                    current.Value.Created,
                    current.Value.Type,
                    current.Value.JsonData,
                    current.Value.JsonMetadata);
                streamEvents.Add(streamEvent);
                maxCount--;
                previous = current;
                current = current.Next;
            }

            bool isEnd = current == null;
            var nextCheckPoint = current?.Value.Checkpoint ?? previous.Value.Checkpoint + 1;

            var page = new AllEventsPage(
                fromCheckpoint,
                nextCheckPoint,
                isEnd,
                direction,
                streamEvents.ToArray());
            return page;
        }

        protected override Task<StreamEventsPage> ReadStreamInternal(
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
                if(stream.IsDeleted)
                {
                    var deleted = new StreamEventsPage(streamId, PageReadStatus.StreamDeleted, start, 0, 0, direction, true);
                    return Task.FromResult(deleted);
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

        protected override async Task<IStreamSubscription> SubscribeToStreamInternal(
            string streamId,
            int startPosition,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var subscription = new StreamSubscription(streamId, startPosition, this, _subscriptions, streamEventReceived, subscriptionDropped, name);
            await subscription.Start(cancellationToken);
            return subscription;
        }

        protected override async Task<IAllStreamSubscription> SubscribeToAllInternal(
            long? fromCheckpoint,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var subscription = new AllStreamSubscription(
                fromCheckpoint,
                this,
                _subscriptions,
                streamEventReceived,
                subscriptionDropped,
                name);

            await subscription.Start(cancellationToken);
            return subscription;
        }

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
                    inMemoryStreamEvent.Checkpoint,
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
            int i = start == StreamVersion.End ? stream.Events.Count - 1 : start;
            while (i >= 0 && count > 0)
            {
                var inMemoryStreamEvent = stream.Events[i];
                var streamEvent = new StreamEvent(
                    streamId,
                    inMemoryStreamEvent.EventId,
                    inMemoryStreamEvent.StreamVersion,
                    inMemoryStreamEvent.Checkpoint,
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