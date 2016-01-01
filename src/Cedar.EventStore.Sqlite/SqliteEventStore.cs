namespace Cedar.EventStore
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using SQLite.Net;
    using SQLite.Net.Interop;

    public class SqliteEventStore : IEventStore
    {
        private readonly SQLiteConnectionPool _connectionPool;
        private readonly Func<SQLiteConnectionWithLock> _getConnection;
        private readonly GetUtcNow _getUtcNow;
        private string _databasePath;

        public SqliteEventStore(ISQLitePlatform sqLitePlatform, string databasePath, GetUtcNow getUtcNow = null)
        {
            Ensure.That(sqLitePlatform, "sqLitePlatform").IsNotNull();
            Ensure.That(databasePath, "databasePath").IsNotNull();

            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            _connectionPool = new SQLiteConnectionPool(sqLitePlatform);
            var connectionString = new SQLiteConnectionString(databasePath, false);
            _getConnection = () => _connectionPool.GetConnection(connectionString);
        }

        public Task AppendToStream(string streamId, int expectedVersion, NewStreamEvent[] events, CancellationToken cancellationToken = default(CancellationToken))
        {
            var connection = _getConnection();
            connection.BeginTransaction();

            //TODO Idempotency check
            /*var sequence = connection.Table<Event>()
                .Where(e => e.BucketId == "default" && e.StreamId == streamId)
                .OrderByDescending(e => e.StreamVersion)
                .Select(e => e.StreamVersion)
                .Take(1)
                .ToList()
                .FirstOrDefault();

            if(sequence != expectedVersion)
            {
                throw new Exception();
            }*/

            var sequence = 0;
            var eventsToInsert = events.Select(e => new SqliteEvent
            {
                JsonData = e.JsonData,
                EventId = e.EventId,
                JsonMetadata = e.JsonMetadata,
                IsDeleted = false,
                OriginalStreamId = streamId,
                SequenceNumber = sequence++,
                Created = _getUtcNow().UtcDateTime,
                StreamId = streamId
            });

            foreach(var eventToInsert in eventsToInsert)
            {
                connection.Insert(eventToInsert);
            }
            connection.Commit();

            return Task.FromResult(0);
        }

        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public Task<AllEventsPage> ReadAll(
            Checkpoint checkpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Reads the stream.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="start">The start.</param>
        /// <param name="count">The count.</param>
        /// <param name="direction">The direction.</param>
        /// <returns></returns>
        public Task<StreamEventsPage> ReadStream(
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return direction == ReadDirection.Forward
                ? ReadSteamForwards(streamId, start, count)
                : ReadSteamBackwards(streamId, start, count);
        }

        public Task<IStreamSubscription> SubscribeToStream(string streamId, EventReceived eventReceived, SubscriptionDropped subscriptionDropped, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {}

        public void Initialize()
        {
            var connection = _getConnection();
            connection.CreateTable<SqliteEvent>();
            connection.CreateIndex("Events", "EventId", true);
            connection.CreateIndex("Events", new[] { "StoreId", "StreamId", "StreamVersion" }, true);
        }

        public void Drop()
        {
            var connection = _getConnection();
            connection.DropTable<SqliteEvent>();
        }

        private Task<StreamEventsPage> ReadSteamForwards(string streamId, int start, int count)
        {
            var connection = _getConnection();

            var results = connection.Table<SqliteEvent>()
                .Where(e => e.StreamId == streamId)
                .OrderBy(e => e.SequenceNumber)
                .Skip(start)
                .Take(count)
                // Must enumerate the results before selecting a StreamEvent else activation
                // exception from Sqlite.Net trying create a StreamEvent. Comment out the 
                // line below if you want to see the test(s) fail.
                .ToArray()
                .Select(@event => @event.ToStreamEvent())
                .ToArray();

            var streamEventsPage = new StreamEventsPage(
                streamId: streamId,
                status: PageReadStatus.Success,
                fromStreamVersion: start,
                nextStreamVersion: results[results.Length - 1].StreamVersion + 1,
                lastStreamVersion: results[results.Length - 1].StreamVersion,
                direction: ReadDirection.Forward,
                //TODO
                isEndOfStream: true,
                events: results);

            return Task.FromResult(streamEventsPage);
        }

        private Task<StreamEventsPage> ReadSteamBackwards(string streamId, int start, int count)
        {
            var connection = _getConnection();

            var results = connection.Table<SqliteEvent>()
                .Where(e => e.StreamId == streamId)
                .OrderByDescending(e => e.SequenceNumber)
                .Skip(start)
                .Take(count)
                // Must enumerate the results before selecting a StreamEvent else activation
                // exception from Sqlite.Net trying create a StreamEvent. Comment out the 
                // line below if you want to see the test(s) fail.
                .ToArray()
                .Select(@event => @event.ToStreamEvent())
                .ToArray();

            var streamEventsPage = new StreamEventsPage(
                streamId: streamId,
                status: PageReadStatus.Success,
                fromStreamVersion: start,
                nextStreamVersion: results[0].StreamVersion - 1,
                lastStreamVersion: results[0].StreamVersion,
                direction: ReadDirection.Backward,
                isEndOfStream: true,
                events: results);

            return Task.FromResult(streamEventsPage);
        }
    }
}