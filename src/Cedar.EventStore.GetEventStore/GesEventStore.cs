namespace Cedar.EventStore.GetEventStore
{
    using System;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Cedar.EventStore.Subscriptions;
    using EnsureThat;
    using global::EventStore.ClientAPI;
    using global::EventStore.ClientAPI.SystemData;
    using ExpectedVersion = Cedar.EventStore.Streams.ExpectedVersion;
    using ReadDirection = Cedar.EventStore.Streams.ReadDirection;
    using GesWrongExpectedVersion=global::EventStore.ClientAPI.Exceptions.WrongExpectedVersionException;

    public class GesEventStore : IEventStore
    {
        private readonly IEventStoreConnection _connection;
        private readonly UserCredentials _userCredentials;
        private static readonly Encoding s_encoding = Encoding.UTF8;
        private bool _disposed;

        public GesEventStore(IEventStoreConnection connection, UserCredentials userCredentials = null)
        {
            _connection = connection;
            _userCredentials = userCredentials;
        }

        public void Dispose()
        {
            _disposed = true;
        }

        public async Task<AllEventsPage> ReadAllForwards(
            long fromCheckpointInclusive,
            int maxCount,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Ensure.That(fromCheckpointInclusive, nameof(fromCheckpointInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            Position position = new Position(fromCheckpointInclusive, fromCheckpointInclusive);
            var slice = await _connection.ReadAllEventsForwardAsync(position, maxCount, true, _userCredentials);

            return new AllEventsPage(
                slice.FromPosition.PreparePosition,
                slice.NextPosition.PreparePosition,
                slice.IsEndOfStream,
                ReadDirection.Forward,
                slice.Events
                    .Where(resolvedEvent => !resolvedEvent.Event.EventStreamId.StartsWith("$"))
                    .Select(resolvedEvent => new StreamEvent(
                        resolvedEvent.Event.EventStreamId,
                        resolvedEvent.Event.EventId,
                        resolvedEvent.Event.EventNumber,
                        resolvedEvent.OriginalPosition.GetValueOrDefault().PreparePosition,
                        FromEpoch(resolvedEvent.Event.CreatedEpoch),
                        resolvedEvent.Event.EventType,
                        s_encoding.GetString(resolvedEvent.Event.Data),
                        s_encoding.GetString(resolvedEvent.Event.Metadata)))
                    .ToArray());
        }

        public Task<AllEventsPage> ReadAllBackwards(
            long fromCheckpointInclusive,
            int maxCount,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Ensure.That(fromCheckpointInclusive, nameof(fromCheckpointInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            throw new NotImplementedException();
        }

        public async Task<StreamEventsPage> ReadStreamForwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(0);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            var slice = await _connection.ReadStreamEventsForwardAsync(streamId, fromVersionInclusive, maxCount, true, _userCredentials);

            return new StreamEventsPage(
                streamId,
                (PageReadStatus) slice.Status,
                fromVersionInclusive,
                slice.NextEventNumber,
                slice.LastEventNumber,
                ReadDirection.Forward,
                slice.IsEndOfStream,
                slice.Events.Select(resolvedEvent => new StreamEvent(
                    streamId,
                    resolvedEvent.Event.EventId,
                    resolvedEvent.Event.EventNumber,
                    resolvedEvent.OriginalPosition.GetValueOrDefault().PreparePosition,
                    FromEpoch(resolvedEvent.Event.CreatedEpoch),
                    resolvedEvent.Event.EventType,
                    s_encoding.GetString(resolvedEvent.Event.Data),
                    s_encoding.GetString(resolvedEvent.Event.Metadata)))
                    .ToArray());
        }

        public Task<StreamEventsPage> ReadStreamBackwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(fromVersionInclusive, nameof(fromVersionInclusive)).IsGte(-1);
            Ensure.That(maxCount, nameof(maxCount)).IsGte(1);

            CheckIfDisposed();

            throw new NotImplementedException();
        }

        public Task<IStreamSubscription> SubscribeToStream(
            string streamId,
            int fromVersionExclusive,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(streamEventReceived, nameof(streamEventReceived)).IsNotNull();

            CheckIfDisposed();

            throw new NotImplementedException();
        }

        public Task<IAllStreamSubscription> SubscribeToAll(
            long? fromCheckpointExclusive,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Ensure.That(streamEventReceived, nameof(streamEventReceived)).IsNotNull();

            CheckIfDisposed();

            throw new NotImplementedException();
        }

        public Task<long> ReadHeadCheckpoint(CancellationToken cancellationToken = new CancellationToken())
        {
            CheckIfDisposed();

            return _connection.ReadAllEventsBackwardAsync(Position.End, 1, true, _userCredentials)
                .ContinueWith(t => t.Result.NextPosition.PreparePosition, cancellationToken);
        }

        public async Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNull();
            Ensure.That(expectedVersion, nameof(expectedVersion)).IsGte(-2);
            Ensure.That(events, nameof(events)).IsNotNull().HasItems();

            CheckIfDisposed();

            try
            {
                var streamEvents = events.Select(
                    e => new EventData(e.EventId,
                        e.Type,
                        true,
                        s_encoding.GetBytes(e.JsonData),
                        s_encoding.GetBytes(e.JsonMetadata)))
                        .ToArray();
                await _connection.AppendToStreamAsync(
                    streamId,
                    expectedVersion,
                    streamEvents,
                    _userCredentials);
            }
            catch(GesWrongExpectedVersion ex)
            {
                throw new WrongExpectedVersionException(Messages.AppendFailedWrongExpectedVersion(streamId, expectedVersion), ex);
            }
        }

        public async Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNull();
            Ensure.That(expectedVersion, nameof(expectedVersion)).IsGte(-2);

            CheckIfDisposed();

            try
            {
                await _connection.DeleteStreamAsync(streamId, expectedVersion, true, _userCredentials);
            }
            catch(GesWrongExpectedVersion ex)
            {
                throw new WrongExpectedVersionException(Messages.AppendFailedWrongExpectedVersion(streamId, expectedVersion), ex);
            }
        }

        protected void CheckIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }


        private static DateTimeOffset FromEpoch(long timestamp)
        {
            return new DateTimeOffset(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(timestamp));
        }
    }
}