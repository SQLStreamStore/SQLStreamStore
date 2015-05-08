namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using EnsureThat;
    using SQLite.Net;
    using SQLite.Net.Interop;

    public class SqliteEventStoreClient : IEventStoreClient
    {
        private readonly GetUtcNow _getUtcNow;
        private readonly Func<SQLiteConnectionWithLock> _getConnection;
        private readonly SQLiteConnectionPool _connectionPool;
        private string _databasePath;

        public SqliteEventStoreClient(ISQLitePlatform sqLitePlatform, string databasePath, GetUtcNow getUtcNow = null)
        {
            Ensure.That(sqLitePlatform, "sqLitePlatform").IsNotNull();
            Ensure.That(databasePath, "databasePath").IsNotNull();

            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            _connectionPool = new SQLiteConnectionPool(sqLitePlatform);
            var connectionString = new SQLiteConnectionString(databasePath, false);
            _getConnection = () => _connectionPool.GetConnection(connectionString);

        }

        public Task AppendToStream(string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events)
        {
            var connection = _getConnection();
            connection.BeginTransaction();

            //TODO Idempotency check
            /*var sequence = connection.Table<Event>()
                .Where(e => e.BucketId == "default" && e.StreamId == streamId)
                .OrderByDescending(e => e.SequenceNumber)
                .Select(e => e.SequenceNumber)
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

            foreach (var eventToInsert in eventsToInsert)
            {
                connection.Insert(eventToInsert);
            }
            connection.Commit();

            return Task.FromResult(0);
        }

        public Task DeleteStream(string streamId, int expectedVersion = ExpectedVersion.Any)
        {
            throw new NotImplementedException();
        }

        public Task<AllEventsPage> ReadAll(string checkpoint, int maxCount, ReadDirection direction = ReadDirection.Forward)
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
            ReadDirection direction = ReadDirection.Forward)
        {
            return direction == ReadDirection.Forward
                ? ReadSteamForwards(streamId, start, count)
                : ReadSteamBackwards(streamId, start, count);
        }

        public void Initialize()
        {
            var connection = _getConnection();
            connection.CreateTable<SqliteEvent>();
            connection.CreateIndex("Events", "EventId", true);
            connection.CreateIndex("Events", new []{ "StoreId", "StreamId", "SequenceNumber"} , true);
        }

        public void Drop()
        {
            var connection = _getConnection();
            connection.DropTable<SqliteEvent>();
        }

        public void Dispose()
        {}

        private Task<StreamEventsPage> ReadSteamForwards(string streamId, int start, int count)
        {
            var connection = _getConnection();

            StreamEvent[] results = connection.Table<SqliteEvent>()
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

            StreamEventsPage streamEventsPage = new StreamEventsPage(
                streamId: streamId,
                status: PageReadStatus.Success,
                fromSequenceNumber: start,
                nextSequenceNumber: results[results.Length - 1].SequenceNumber + 1,
                lastSequenceNumber: results[results.Length - 1].SequenceNumber,
                direction: ReadDirection.Forward, //TODO
                isEndOfStream: true, events: results);

            return Task.FromResult(streamEventsPage);
        }

        private Task<StreamEventsPage> ReadSteamBackwards(string streamId, int start, int count)
        {
            var connection = _getConnection();

            StreamEvent[] results = connection.Table<SqliteEvent>()
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

            StreamEventsPage streamEventsPage = new StreamEventsPage(
                streamId: streamId,
                status: PageReadStatus.Success,
                fromSequenceNumber: start,
                nextSequenceNumber: results[0].SequenceNumber - 1,
                lastSequenceNumber: results[0].SequenceNumber,
                direction: ReadDirection.Backward,
                isEndOfStream: true,
                events: results);

            return Task.FromResult(streamEventsPage);
        }

        /*private class StreamEventsPage : IStreamEventsPage
        {
            private readonly int _fromSequenceNumber;
            private readonly bool _isEndOfStream;
            private readonly int _lastSequenceNumber;
            private readonly int _nextSequenceNumber;
            private readonly ReadDirection _readDirection;
            private readonly PageReadStatus _status;
            private readonly string _streamId;
            private readonly IReadOnlyCollection<IStreamEvent> _events; 

            public StreamEventsPage(
                string streamId,
                PageReadStatus pageReadStatus,
                ReadDirection readDirection,
                int fromSequenceNumber,
                int lastSequenceNumber,
                int nextSequenceNumber,
                bool isEndOfStream,
                Event[] events)
            {
                _streamId = streamId;
                _status = pageReadStatus;
                _readDirection = readDirection;
                _fromSequenceNumber = fromSequenceNumber;
                _lastSequenceNumber = lastSequenceNumber;
                _nextSequenceNumber = nextSequenceNumber;
                _isEndOfStream = isEndOfStream;

                var streamEvents = events
                    .Select(e => new StreamEvents(e))
                    .Cast<IStreamEvent>()
                    .ToList();

                _events = new ReadOnlyCollection<IStreamEvent>(streamEvents);
            }

            public IReadOnlyCollection<IStreamEvent> Events
            {
                get { return _events; }
            }

            public int FromSequenceNumber
            {
                get { return _fromSequenceNumber; }
            }

            public bool IsEndOfStream
            {
                get { return _isEndOfStream; }
            }

            public int LastSequenceNumber
            {
                get { return _lastSequenceNumber; }
            }

            public int NextSequenceNumber
            {
                get { return _nextSequenceNumber; }
            }

            public ReadDirection ReadDirection
            {
                get { return _readDirection; }
            }

            public PageReadStatus Status
            {
                get { return _status; }
            }

            public string StreamId
            {
                get { return _streamId; }
            }

            private class StreamEvents : IStreamEvent
            {
                private readonly byte[] _body;
                private readonly Guid _eventId;
                private readonly byte[] _headers;
                private readonly int _sequenceNumber;
                private readonly string _streamId;

                public StreamEvents(Event @event)
                {
                    _eventId = @event.EventId;
                    _body = @event.Body;
                    _sequenceNumber = @event.SequenceNumber;
                    _streamId = @event.StreamId;
                    _headers = @event.Headers;
                }

                public IReadOnlyCollection<byte> Body
                {
                    get { return _body; }
                }

                public Guid EventId
                {
                    get { return _eventId; }
                }

                public IReadOnlyCollection<byte> Headers
                {
                    get { return _headers; }
                }

                public int SequenceNumber
                {
                    get { return _sequenceNumber; }
                }

                public string StreamId
                {
                    get { return _streamId; }
                }
            }
        }*/
    }
}
