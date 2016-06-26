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
        private readonly InMemoryAllStream _allStream = new InMemoryAllStream();
        private readonly GetUtcNow _getUtcNow;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        private readonly Action _onStreamAppended;
        private readonly InMemoryStreams _streams = new InMemoryStreams();
        private readonly Subject<Unit> _subscriptions = new Subject<Unit>();
        private bool _isDisposed;

        public InMemoryEventStore(GetUtcNow getUtcNow = null, string logName = null)
            : base(logName ?? nameof(InMemoryEventStore))
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

        protected override void Dispose(bool disposing)
        {
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
            CancellationToken cancellationToken)
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventStore));
            }

            _lock.EnterWriteLock();
            try
            {
                 return AppendToStreamInternal(streamId, expectedVersion, events);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        private Task AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events)
        {
            InMemoryStream inMemoryStream;
            if (expectedVersion == ExpectedVersion.NoStream || expectedVersion == ExpectedVersion.Any)
            {
                if (_streams.TryGetValue(streamId, out inMemoryStream))
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


        protected override async Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventStore));
            }

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
                    return;
                }
                if(expectedVersion != ExpectedVersion.Any &&
                    _streams[streamId].Events.Last().StreamVersion != expectedVersion)
                {
                    throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion(streamId, expectedVersion));
                }
                InMemoryStream inMemoryStream;
                _streams.TryRemove(streamId, out inMemoryStream);
                inMemoryStream.DeleteEvents(ExpectedVersion.Any);
                var streamDeletedEvent = Deleted.CreateStreamDeletedEvent(streamId);
                await AppendToStreamInternal(Deleted.StreamId, ExpectedVersion.Any, new[] { streamDeletedEvent });
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        protected override Task<AllEventsPage> ReadAllForwardsInternal(
            long fromCheckpointExlusive,
            int maxCount,
            CancellationToken cancellationToken)
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventStore));
            }

            _lock.EnterReadLock();
            try
            {
                // Find the node to start from (it may not be equal to the exact checkpoint)
                var current = _allStream.First;
                if(current.Next == null) //Empty store
                {
                    return Task.FromResult(
                        new AllEventsPage(Checkpoint.Start, Checkpoint.Start, true, ReadDirection.Forward));
                }

                var previous = current.Previous;
                while(current.Value.Checkpoint < fromCheckpointExlusive)
                {
                    if(current.Next == null) // fromCheckpoint is past end of store
                    {
                        return Task.FromResult(
                            new AllEventsPage(fromCheckpointExlusive,
                                fromCheckpointExlusive,
                                true,
                                ReadDirection.Forward));
                    }
                    previous = current;
                    current = current.Next;
                }

                var streamEvents = new List<StreamEvent>();
                while(maxCount > 0 && current != null)
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

                var isEnd = current == null;
                var nextCheckPoint = current?.Value.Checkpoint ?? previous.Value.Checkpoint + 1;
                fromCheckpointExlusive = streamEvents.Any() ? streamEvents[0].Checkpoint : 0;

                var page = new AllEventsPage(
                    fromCheckpointExlusive,
                    nextCheckPoint,
                    isEnd,
                    ReadDirection.Forward,
                    streamEvents.ToArray());

                return Task.FromResult(page);
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        protected override Task<AllEventsPage> ReadAllBackwardsInternal(
            long fromCheckpointExclusive,
            int maxCount,
            CancellationToken cancellationToken)
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventStore));
            }

            _lock.EnterReadLock();
            try
            {
                if(fromCheckpointExclusive == Checkpoint.End)
                {
                    fromCheckpointExclusive = _allStream.Last.Value.Checkpoint;
                }

                // Find the node to start from (it may not be equal to the exact checkpoint)
                var current = _allStream.First;
                if(current.Next == null) //Empty store
                {
                    return Task.FromResult(
                        new AllEventsPage(Checkpoint.Start, Checkpoint.Start, true, ReadDirection.Backward));
                }

                var previous = current.Previous;
                while(current.Value.Checkpoint < fromCheckpointExclusive)
                {
                    if(current.Next == null) // fromCheckpoint is past end of store
                    {
                        return Task.FromResult(
                            new AllEventsPage(fromCheckpointExclusive,
                                fromCheckpointExclusive,
                                true,
                                ReadDirection.Backward));
                    }
                    previous = current;
                    current = current.Next;
                }

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

                fromCheckpointExclusive = streamEvents.Any() ? streamEvents[0].Checkpoint : 0;

                var page = new AllEventsPage(
                    fromCheckpointExclusive,
                    nextCheckPoint,
                    isEnd,
                    ReadDirection.Backward,
                    streamEvents.ToArray());

                return Task.FromResult(page);
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        protected override Task<StreamEventsPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            CancellationToken cancellationToken)
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventStore));
            }

            _lock.EnterReadLock();
            try
            {
                InMemoryStream stream;
                if(!_streams.TryGetValue(streamId, out stream))
                {
                    var notFound = new StreamEventsPage(streamId,
                        PageReadStatus.StreamNotFound,
                        start,
                        -1,
                        -1,
                        ReadDirection.Forward,
                        true);
                    return Task.FromResult(notFound);
                }

                var events = new List<StreamEvent>();
                var i = start;

                while(i < stream.Events.Count && count > 0)
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

                var lastStreamVersion = stream.Events.Last().StreamVersion;
                var nextStreamVersion = events.Last().StreamVersion + 1;
                var endOfStream = i == stream.Events.Count;

                var page = new StreamEventsPage(
                    streamId,
                    PageReadStatus.Success,
                    start,
                    nextStreamVersion,
                    lastStreamVersion,
                    ReadDirection.Forward,
                    endOfStream,
                    events.ToArray());

                return Task.FromResult(page);
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        protected override Task<StreamEventsPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            CancellationToken cancellationToken)
        {
            if(_isDisposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryEventStore));
            }

            _lock.EnterReadLock();
            try
            {
                InMemoryStream stream;
                if(!_streams.TryGetValue(streamId, out stream))
                {
                    var notFound = new StreamEventsPage(streamId,
                        PageReadStatus.StreamNotFound,
                        fromVersionInclusive,
                        -1,
                        -1,
                        ReadDirection.Backward,
                        true);
                    return Task.FromResult(notFound);
                }

                var events = new List<StreamEvent>();
                var i = fromVersionInclusive == StreamVersion.End ? stream.Events.Count - 1 : fromVersionInclusive;
                while(i >= 0 && count > 0)
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

                var lastStreamVersion = stream.Events.Last().StreamVersion;
                var nextStreamVersion = events.Last().StreamVersion - 1;
                var endOfStream = nextStreamVersion < 0;

                var page = new StreamEventsPage(
                    streamId,
                    PageReadStatus.Success,
                    fromVersionInclusive,
                    nextStreamVersion,
                    lastStreamVersion,
                    ReadDirection.Backward,
                    endOfStream,
                    events.ToArray());

                return Task.FromResult(page);
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        protected override async Task<IStreamSubscription> SubscribeToStreamInternal(
            string streamId,
            int startVersion,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped,
            string name,
            CancellationToken cancellationToken)
        {
            var subscription = new StreamSubscription(
                streamId,
                startVersion,
                this,
                _subscriptions,
                streamEventReceived,
                subscriptionDropped,
                name);
            await subscription.Start(cancellationToken);
            return subscription;
        }

        protected override Task<long> ReadHeadCheckpointInternal(CancellationToken cancellationToken)
        {
            var streamEvent = _allStream.LastOrDefault();
            return streamEvent == null ? Task.FromResult(-1L) : Task.FromResult(streamEvent.Checkpoint);
        }

        protected override async Task<IAllStreamSubscription> SubscribeToAllInternal(
            long? fromCheckpoint,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped,
            string name,
            CancellationToken cancellationToken)
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
    }
}