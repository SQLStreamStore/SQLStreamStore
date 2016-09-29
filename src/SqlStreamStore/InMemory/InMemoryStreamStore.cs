// ReSharper disable once CheckNamespace
namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.InMemory;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;
    using StreamStoreStore.Json;
    using static Streams.Deleted;

    /// <summary>
    ///     Represents an in-memory implementation of a stream store. Use for testing or high/speed + volatile scenarios.
    /// </summary>
    public sealed class InMemoryStreamStore : StreamStoreBase
    {
        private readonly InMemoryAllStream _allStream = new InMemoryAllStream();
        private readonly GetUtcNow _getUtcNow;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private readonly Action _onStreamAppended;
        private readonly Dictionary<string, InMemoryStream> _streams = new Dictionary<string, InMemoryStream>();
        private readonly Subject<Unit> _subscriptions = new Subject<Unit>();
        private bool _isDisposed;
        private int _currentPosition = 0;

        public InMemoryStreamStore(GetUtcNow getUtcNow = null, string logName = null)
            : base(TimeSpan.FromMinutes(1), 10000, getUtcNow, logName ?? nameof(InMemoryStreamStore))
        {
            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            _allStream.AddFirst(new InMemoryStreamMessage(
                "<in-memory-root-message>",
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
            using(_lock.UseWriteLock())
            {
                _subscriptions.OnCompleted();
                _allStream.Clear();
                _streams.Clear();
                _isDisposed = true;
            }
        }

        public override Task InitializeStore(
                bool ignoreErrors = false,
                CancellationToken cancellationToken = default(CancellationToken))
            => Task.FromResult(0);

        public override Task<int> GetmessageCount(string streamId, CancellationToken cancellationToken = new CancellationToken())
        {
            using(_lock.UseReadLock())
            {
                return Task.FromResult(!_streams.ContainsKey(streamId) ? 0 : _streams[streamId].Events.Count);
            }
        }

        protected override async Task AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(_lock.UseWriteLock())
            {
                AppendToStreamInternal(streamId, expectedVersion, messages);
            }
            var result = await GetStreamMetadataInternal(streamId, cancellationToken);
            await CheckStreamMaxCount(streamId, result.MaxCount, cancellationToken);
        }

        private async Task CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = await GetmessageCount(streamId, cancellationToken);
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamMessagesPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start,
                        toPurge, cancellationToken);

                    if (streamMessagesPage.Status == PageReadStatus.Success)
                    {
                        foreach (var message in streamMessagesPage.Messages)
                        {
                            await DeleteEventInternal(streamId, message.MessageId, cancellationToken);
                        }
                    }
                }
            }
        }

        private void AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages)
        {
            InMemoryStream inMemoryStream;
            if (expectedVersion == ExpectedVersion.NoStream || expectedVersion == ExpectedVersion.Any)
            {
                if (_streams.TryGetValue(streamId, out inMemoryStream))
                {
                    inMemoryStream.AppendToStream(expectedVersion, messages);
                }
                else
                {
                    inMemoryStream = new InMemoryStream(
                        streamId,
                        _allStream,
                        _getUtcNow,
                        _onStreamAppended,
                        () => _currentPosition++);
                    inMemoryStream.AppendToStream(expectedVersion, messages);
                    _streams.Add(streamId, inMemoryStream);
                }
                return;
            }

            if (!_streams.TryGetValue(streamId, out inMemoryStream))
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(streamId, expectedVersion));
            }
            inMemoryStream.AppendToStream(expectedVersion, messages);
        }

        protected override Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using (_lock.UseWriteLock())
            {
                if (!_streams.ContainsKey(streamId))
                {
                    return Task.FromResult(0);
                }

                var inMemoryStream = _streams[streamId];
                bool deleted = inMemoryStream.DeleteEvent(eventId);

                if (deleted)
                {
                    var eventDeletedEvent = CreateMessageDeletedMessage(streamId, eventId);
                    AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { eventDeletedEvent });
                }

                return Task.FromResult(0);
            }
        }

        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            using (_lock.UseReadLock())
            {
                string metaStreamId = $"$${streamId}";

                var eventsPage = await ReadStreamBackwardsInternal(metaStreamId, StreamVersion.End,
                    1, cancellationToken);

                if (eventsPage.Status == PageReadStatus.StreamNotFound)
                {
                    return new StreamMetadataResult(streamId, -1);
                }

                var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(
                    eventsPage.Messages[0].JsonData);

                return new StreamMetadataResult(
                    streamId,
                    eventsPage.LastStreamVersion,
                    metadataMessage.MaxAge,
                    metadataMessage.MaxCount,
                    metadataMessage.MetaJson);
            }
        }

        protected override async Task SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            using(_lock.UseWriteLock())
            {
                string metaStreamId = $"$${streamId}";

                var metadataMessage = new MetadataMessage
                {
                    StreamId = streamId,
                    MaxAge = maxAge,
                    MaxCount = maxCount,
                    MetaJson = metadataJson
                };
                var json = SimpleJson.SerializeObject(metadataMessage);
                var newmessage = new NewStreamMessage(Guid.NewGuid(), "$stream-metadata", json);

                AppendToStreamInternal(metaStreamId, expectedStreamMetadataVersion, new[] { newmessage });

                await CheckStreamMaxCount(streamId, metadataMessage.MaxCount, cancellationToken);
            }
        }

        protected override
            Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(_lock.UseWriteLock())
            {
                DeleteStream(streamId, expectedVersion);

                // Delete metadata stream, if it exists
                DeleteStream($"$${streamId}", ExpectedVersion.Any);

                return Task.FromResult(0);
            }
        }

        private void DeleteStream(string streamId, int expectedVersion)
        {
            if (!_streams.ContainsKey(streamId))
            {
                if (expectedVersion >= 0)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId, expectedVersion));
                }
                return;
            }
            if (expectedVersion != ExpectedVersion.Any &&
                _streams[streamId].Events.Last().StreamVersion != expectedVersion)
            {
                throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId, expectedVersion));
            }
            InMemoryStream inMemoryStream = _streams[streamId];
            _streams.Remove(streamId);
            inMemoryStream.DeleteAllEvents(ExpectedVersion.Any);

            var streamDeletedEvent = CreateStreamDeletedMessage(streamId);
            AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { streamDeletedEvent });
        }

        protected override Task<AllMessagesPage> ReadAllForwardsInternal(
            long fromPositionExlusive,
            int maxCount,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(_lock.UseReadLock())
            {
                // Find the node to start from (it may not be equal to the exact position)
                var current = _allStream.First;
                if(current.Next == null) //Empty store
                {
                    return Task.FromResult(
                        new AllMessagesPage(Position.Start, Position.Start, true, ReadDirection.Forward));
                }

                var previous = current.Previous;
                while(current.Value.Position < fromPositionExlusive)
                {
                    if(current.Next == null) // fromPosition is past end of store
                    {
                        return Task.FromResult(
                            new AllMessagesPage(fromPositionExlusive,
                                fromPositionExlusive,
                                true,
                                ReadDirection.Forward));
                    }
                    previous = current;
                    current = current.Next;
                }

                var messages = new List<StreamMessage>();
                while(maxCount > 0 && current != null)
                {
                    var message = new StreamMessage(
                        current.Value.StreamId,
                        current.Value.MessageId,
                        current.Value.StreamVersion,
                        current.Value.Position,
                        current.Value.Created,
                        current.Value.Type,
                        current.Value.JsonData,
                        current.Value.JsonMetadata);
                    messages.Add(message);
                    maxCount--;
                    previous = current;
                    current = current.Next;
                }

                var isEnd = current == null;
                var nextCheckPoint = current?.Value.Position ?? previous.Value.Position + 1;
                fromPositionExlusive = messages.Any() ? messages[0].Position : 0;

                var page = new AllMessagesPage(
                    fromPositionExlusive,
                    nextCheckPoint,
                    isEnd,
                    ReadDirection.Forward,
                    messages.ToArray());

                return Task.FromResult(page);
            }
        }

        protected override Task<AllMessagesPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using (_lock.UseReadLock())
            {
                if (fromPositionExclusive == Position.End)
                {
                    fromPositionExclusive = _allStream.Last.Value.Position;
                }

                // Find the node to start from (it may not be equal to the exact position)
                var current = _allStream.First;
                if(current.Next == null) //Empty store
                {
                    return Task.FromResult(
                        new AllMessagesPage(Position.Start, Position.Start, true, ReadDirection.Backward));
                }

                var previous = current.Previous;
                while(current.Value.Position < fromPositionExclusive)
                {
                    if(current.Next == null) // fromPosition is past end of store
                    {
                        return Task.FromResult(
                            new AllMessagesPage(fromPositionExclusive,
                                fromPositionExclusive,
                                true,
                                ReadDirection.Backward));
                    }
                    previous = current;
                    current = current.Next;
                }

                var messages = new List<StreamMessage>();
                while(maxCount > 0 && current != _allStream.First)
                {
                    var message = new StreamMessage(
                        current.Value.StreamId,
                        current.Value.MessageId,
                        current.Value.StreamVersion,
                        current.Value.Position,
                        current.Value.Created,
                        current.Value.Type,
                        current.Value.JsonData,
                        current.Value.JsonMetadata);
                    messages.Add(message);
                    maxCount--;
                    previous = current;
                    current = current.Previous;
                }

                bool isEnd;
                if(previous == null || previous.Value.Position == 0)
                {
                    isEnd = true;
                }
                else
                {
                    isEnd = false;
                }
                var nextCheckPoint = isEnd
                    ? 0
                    : current.Value.Position;

                fromPositionExclusive = messages.Any() ? messages[0].Position : 0;

                var page = new AllMessagesPage(
                    fromPositionExclusive,
                    nextCheckPoint,
                    isEnd,
                    ReadDirection.Backward,
                    messages.ToArray());

                return Task.FromResult(page);
            }
        }

        protected override Task<StreamMessagesPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(_lock.UseReadLock())
            {
                InMemoryStream stream;
                if(!_streams.TryGetValue(streamId, out stream))
                {
                    var notFound = new StreamMessagesPage(streamId,
                        PageReadStatus.StreamNotFound,
                        start,
                        -1,
                        -1,
                        ReadDirection.Forward,
                        true);
                    return Task.FromResult(notFound);
                }

                var events = new List<StreamMessage>();
                var i = start;

                while(i < stream.Events.Count && count > 0)
                {
                    var inMemorymessage = stream.Events[i];
                    var message = new StreamMessage(
                        streamId,
                        inMemorymessage.MessageId,
                        inMemorymessage.StreamVersion,
                        inMemorymessage.Position,
                        inMemorymessage.Created,
                        inMemorymessage.Type,
                        inMemorymessage.JsonData,
                        inMemorymessage.JsonMetadata);
                    events.Add(message);

                    i++;
                    count--;
                }

                var lastStreamVersion = stream.Events.Last().StreamVersion;
                var nextStreamVersion = events.Count == 0 ? lastStreamVersion + 1 : events.Last().StreamVersion + 1;
                var endOfStream = i == stream.Events.Count;

                var page = new StreamMessagesPage(
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
        }

        protected override Task<StreamMessagesPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using (_lock.UseReadLock())
            {
                InMemoryStream stream;
                if (!_streams.TryGetValue(streamId, out stream))
                {
                    var notFound = new StreamMessagesPage(streamId,
                        PageReadStatus.StreamNotFound,
                        fromVersionInclusive,
                        -1,
                        -1,
                        ReadDirection.Backward,
                        true);
                    return Task.FromResult(notFound);
                }

                var events = new List<StreamMessage>();
                var i = fromVersionInclusive == StreamVersion.End ? stream.Events.Count - 1 : fromVersionInclusive;
                while (i >= 0 && count > 0)
                {
                    var inMemorymessage = stream.Events[i];
                    var message = new StreamMessage(
                        streamId,
                        inMemorymessage.MessageId,
                        inMemorymessage.StreamVersion,
                        inMemorymessage.Position,
                        inMemorymessage.Created,
                        inMemorymessage.Type,
                        inMemorymessage.JsonData,
                        inMemorymessage.JsonMetadata);
                    events.Add(message);

                    i--;
                    count--;
                }

                var lastStreamVersion = stream.Events.Last().StreamVersion;
                var nextStreamVersion = events.Last().StreamVersion - 1;
                var endOfStream = nextStreamVersion < 0;

                var page = new StreamMessagesPage(
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
        }

        protected override IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name)
        {
            IStreamSubscription subscription = new StreamSubscription(
                streamId,
                startVersion,
                this,
                _subscriptions,
                streamMessageReceived,
                subscriptionDropped,
                name);
            return subscription;
        }

        protected override Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            var message = _allStream.LastOrDefault();
            return message == null ? Task.FromResult(-1L) : Task.FromResult(message.Position);
        }

        protected override IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            string name)
        {
            return new AllStreamSubscription(
                fromPosition,
                this,
                _subscriptions,
                streamMessageReceived,
                subscriptionDropped,
                name);
        }

        private void GuardAgainstDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(InMemoryStreamStore));
            }
        }
    }
}